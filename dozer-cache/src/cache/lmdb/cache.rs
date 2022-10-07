use std::sync::Arc;

use anyhow::bail;
use lmdb::{Cursor, Database, Environment, RoTransaction, RwTransaction, Transaction, WriteFlags};

use dozer_schema::registry::context::Context;
use dozer_schema::registry::SchemaRegistryClient;
use dozer_schema::storage::get_schema_key;
use dozer_types::types::Record;
use dozer_types::types::{Schema, SchemaIdentifier};

use crate::cache::expression::Expression;
use crate::cache::get_primary_key;

use super::super::Cache;
use super::indexer::Indexer;
use super::query::QueryHandler;
use super::utils;

pub struct LmdbCache {
    env: Environment,
    db: Database,
}

async fn _get_schema_from_registry(
    client: Arc<SchemaRegistryClient>,
    schema_identifier: SchemaIdentifier,
) -> anyhow::Result<Schema> {
    let ctx = Context::current();
    let schema = client.get(ctx, schema_identifier).await?;
    Ok(schema)
}

impl LmdbCache {
    pub fn new(temp_storage: bool) -> Self {
        let (env, db) = utils::init_db(temp_storage).unwrap();
        Self { env, db }
    }

    fn _insert(&self, txn: &mut RwTransaction, rec: Record, schema: Schema) -> anyhow::Result<()> {
        // let p_key = schema.primary_index.clone();
        let p_key = vec![1];
        let values = rec.values.clone();
        let key = get_primary_key(p_key, values.to_owned());
        let encoded: Vec<u8> = bincode::serialize(&rec).unwrap();

        txn.put::<Vec<u8>, Vec<u8>>(self.db, &key, &encoded, WriteFlags::default())?;

        let indexer = Indexer::new(&self.db);

        indexer.build_indexes(txn, rec, schema, key)?;

        Ok(())
    }

    fn _insert_schema(&self, txn: &mut RwTransaction, schema: Schema) -> anyhow::Result<()> {
        let key = get_schema_key(schema.identifier.clone().unwrap());
        let encoded: Vec<u8> = bincode::serialize(&schema)?;
        txn.put::<Vec<u8>, Vec<u8>>(self.db, &key, &encoded, WriteFlags::default())?;
        Ok(())
    }

    fn _debug_dump(&self) -> anyhow::Result<()> {
        let txn: RoTransaction = self.env.begin_ro_txn().unwrap();
        let cursor = txn.open_ro_cursor(self.db)?;
        loop {
            if let Ok((key, val)) = cursor.get(None, None, 8) {
                println!("key: {:?}, val: {:?}", key.unwrap(), val);
            } else {
                break;
            };
        }
        Ok(())
    }
}

impl Cache for LmdbCache {
    fn insert(&self, rec: Record, schema: Schema) -> anyhow::Result<()> {
        if rec.schema_id != schema.identifier {
            bail!("record and schema dont have the same id.");
        }

        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        let schema_identifier = match schema.identifier.clone() {
            Some(id) => id,
            None => bail!("cache::Insert - Schema Id is not present"),
        };
        match self.get_schema(schema_identifier) {
            Ok(_schema) => {}
            Err(_) => {
                self._insert_schema(&mut txn, schema.clone())?;
            }
        };

        self._insert(&mut txn, rec.clone(), schema)?;
        txn.commit()?;
        Ok(())
    }

    fn delete(&self, key: Vec<u8>) -> anyhow::Result<()> {
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        txn.del(self.db, &key, None)?;
        txn.commit()?;
        Ok(())
    }

    fn get(&self, key: Vec<u8>) -> anyhow::Result<Record> {
        let txn: RoTransaction = self.env.begin_ro_txn()?;
        let rec = txn.get(self.db, &key)?;
        let rec: Record = bincode::deserialize(rec)?;
        Ok(rec)
    }

    fn query(
        &self,
        schema_identifier: SchemaIdentifier,
        exp: Expression,
    ) -> anyhow::Result<Vec<Record>> {
        let schema = self.get_schema(schema_identifier.clone())?;

        let handler = QueryHandler::new(&self.env, &self.db);
        let pkeys = handler.query(schema, exp)?;
        let mut records = vec![];
        for key in pkeys.iter() {
            let key = key.clone();
            let rec = self.get(key)?;
            records.push(rec);
        }
        Ok(records)
    }

    fn update(&self, key: Vec<u8>, rec: Record, schema: Schema) -> anyhow::Result<()> {
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        txn.del(self.db, &key, None)?;

        self._insert(&mut txn, rec, schema)?;
        txn.commit()?;
        Ok(())
    }

    fn get_schema(&self, schema_identifier: SchemaIdentifier) -> anyhow::Result<Schema> {
        let txn: RoTransaction = self.env.begin_ro_txn()?;

        let key = get_schema_key(schema_identifier.clone());
        let schema = txn.get(self.db, &key)?;
        let schema: Schema = bincode::deserialize(schema)?;
        Ok(schema)
    }
    fn insert_schema(&self, schema: Schema) -> anyhow::Result<()> {
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        self._insert_schema(&mut txn, schema)?;
        txn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dozer_schema::{
        registry::{_serve_channel, client, SchemaRegistryClient},
        test_helper::init_schema,
    };
    use dozer_types::types::{Field, Record, Schema};

    use crate::cache::{
        expression::{self, Expression},
        get_primary_key, Cache,
    };

    use super::LmdbCache;

    async fn _setup() -> (LmdbCache, Schema) {
        let client_transport = _serve_channel().unwrap();
        let client = Arc::new(
            SchemaRegistryClient::new(client::Config::default(), client_transport).spawn(),
        );
        let schema = init_schema(client.clone()).await;
        let cache = LmdbCache::new(true);
        (cache, schema)
    }

    #[tokio::test]
    async fn insert_and_get_schema() -> anyhow::Result<()> {
        let (cache, schema) = _setup().await;
        cache.insert_schema(schema.clone())?;

        let get_schema = cache.get_schema(schema.identifier.clone().unwrap())?;
        assert_eq!(get_schema, schema, "must be equal");
        Ok(())
    }

    #[tokio::test]
    async fn insert_get_and_delete_record() -> anyhow::Result<()> {
        let val = "bar".to_string();
        let (cache, schema) = _setup().await;
        let record = Record::new(schema.identifier.clone(), vec![Field::String(val.clone())]);
        cache.insert(record.clone(), schema)?;

        let key = get_primary_key(vec![0], vec![Field::String(val)]);

        let get_record = cache.get(key.clone())?;
        assert_eq!(get_record, record.clone(), "must be equal");

        cache.delete(key.clone())?;

        cache.get(key).expect_err("Must not find a record");

        Ok(())
    }

    #[tokio::test]
    async fn insert_and_query_record() -> anyhow::Result<()> {
        let val = "bar".to_string();
        let (cache, schema) = _setup().await;
        let record = Record::new(schema.identifier.clone(), vec![Field::String(val.clone())]);

        cache.insert(record.clone(), schema.clone())?;

        let exp = Expression::Simple(
            "foo".to_string(),
            expression::Comparator::EQ,
            Field::String("bar".to_string()),
        );
        let records = cache.query(schema.identifier.unwrap(), exp)?;

        println!("records: {:?}", records);
        assert_eq!(records[0], record.clone(), "must be equal");

        Ok(())
    }
}
