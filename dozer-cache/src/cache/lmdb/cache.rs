use std::sync::Arc;

use super::super::Cache;
use super::utils;
use async_trait::async_trait;
use dozer_schema::registry::context::Context;
use dozer_schema::registry::SchemaRegistryClient;
use dozer_schema::storage::get_schema_key;
use dozer_types::types::{Field, Schema, SchemaIdentifier};
use dozer_types::types::{Operation, Record};
use lmdb::{Database, Environment, RoTransaction, RwTransaction, Transaction, WriteFlags};

pub struct LmdbCache {
    env: Environment,
    db: Database,
    client: Arc<SchemaRegistryClient>,
}

async fn get_schema_from_registry(
    client: Arc<SchemaRegistryClient>,
    schema_identifier: SchemaIdentifier,
) -> anyhow::Result<Schema> {
    let ctx = Context::current();
    let schema = client.get(ctx, schema_identifier).await?;
    Ok(schema)
}

impl LmdbCache {
    pub fn new(client: Arc<SchemaRegistryClient>, temp_storage: bool) -> Self {
        let (env, db) = utils::init_db(temp_storage);
        Self {
            env,
            db,
            client: client.clone(),
        }
    }

    fn _insert(&self, rec: Record, schema: Schema) -> anyhow::Result<()> {
        let mut txn: RwTransaction = self.env.begin_rw_txn().unwrap();
        let p_key = schema.primary_index.clone();
        let values = rec.values.clone();
        let key = self.get_key(p_key, values.to_owned());

        let encoded: Vec<u8> = bincode::serialize(&rec).unwrap();

        txn.put::<Vec<u8>, Vec<u8>>(self.db, &key, &encoded, WriteFlags::default())?;

        txn.commit().unwrap();

        Ok(())
    }
}

#[async_trait]
impl Cache for LmdbCache {
    fn get_key(&self, primary_index: Vec<usize>, values: Vec<Field>) -> Vec<u8> {
        let key: Vec<Vec<u8>> = primary_index
            .iter()
            .map(|idx| {
                let field = values[*idx].clone();
                let encoded: Vec<u8> = bincode::serialize(&field).unwrap();
                encoded
            })
            .collect();

        key.join("#".as_bytes())
    }
    async fn insert(&self, rec: Record) -> anyhow::Result<()> {
        let schema_identifier = rec.schema_id.clone().unwrap();
        let schema = match self.get_schema(schema_identifier.clone()).await {
            Ok(schema) => schema,
            Err(_) => {
                let client = self.client.clone();
                let id = schema_identifier.clone();
                let schema = tokio::spawn(async move {
                    let schema = get_schema_from_registry(client, id).await.unwrap();
                    schema
                })
                .await?;
                self.insert_schema(schema.clone()).await?;
                schema
            }
        };
        self._insert(rec, schema)
    }

    async fn delete(&self, key: Vec<u8>) -> anyhow::Result<()> {
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        txn.del(self.db, &key, None)?;
        txn.commit()?;
        Ok(())
    }

    async fn get(&self, key: Vec<u8>) -> anyhow::Result<Record> {
        let txn: RoTransaction = self.env.begin_ro_txn()?;
        let rec = txn.get(self.db, &key)?;
        let rec: Record = bincode::deserialize(rec)?;
        Ok(rec)
    }

    async fn query(&self, _key: Vec<u8>) -> anyhow::Result<Vec<Record>> {
        todo!()
    }

    async fn handle_batch(&self, _operations: Vec<Operation>) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_schema(&self, schema_identifier: SchemaIdentifier) -> anyhow::Result<Schema> {
        let txn: RoTransaction = self.env.begin_ro_txn()?;

        let key = get_schema_key(schema_identifier.clone());
        let schema = txn.get(self.db, &key)?;
        let schema: Schema = bincode::deserialize(schema)?;
        Ok(schema)
    }
    async fn insert_schema(&self, schema: Schema) -> anyhow::Result<()> {
        let key = get_schema_key(schema.identifier.clone().unwrap());
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        let encoded: Vec<u8> = bincode::serialize(&schema)?;
        txn.put::<Vec<u8>, Vec<u8>>(self.db, &key, &encoded, WriteFlags::default())?;
        txn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::LmdbCache;
    use crate::cache::Cache;
    use dozer_schema::{
        registry::{SchemaRegistryClient, _serve_channel, client},
        test_helper::init_schema,
    };
    use dozer_types::types::{Field, Record};

    #[tokio::test]
    async fn insert_and_get_schema() -> anyhow::Result<()> {
        let client_transport = _serve_channel().unwrap();
        let client = Arc::new(
            SchemaRegistryClient::new(client::Config::default(), client_transport).spawn(),
        );
        let schema = init_schema(client.clone()).await;
        let cache = LmdbCache::new(client.clone(), true);
        cache.insert_schema(schema.clone()).await?;

        let get_schema = cache.get_schema(schema.identifier.clone().unwrap()).await?;
        assert_eq!(get_schema, schema, "must be equal");
        Ok(())
    }
    #[tokio::test]
    async fn insert_get_and_delete_record() -> anyhow::Result<()> {
        let val = "bar".to_string();

        let client_transport = _serve_channel().unwrap();
        let client = Arc::new(
            SchemaRegistryClient::new(client::Config::default(), client_transport).spawn(),
        );
        let schema = init_schema(client.clone()).await;

        let record = Record::new(schema.identifier.clone(), vec![Field::String(val.clone())]);

        let cache = LmdbCache::new(client.clone(), true);

        cache.insert(record.clone()).await?;

        let key = cache.get_key(vec![0], vec![Field::String(val)]);

        let get_record = cache.get(key.clone()).await?;
        assert_eq!(get_record, record.clone(), "must be equal");

        cache.delete(key.clone()).await?;

        cache.get(key).await.expect_err("Must not find a record");

        Ok(())
    }
}
