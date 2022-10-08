use std::sync::Arc;

use anyhow::{bail, Context};
use lmdb::{
    Cursor, Database, Environment, RoCursor, RoTransaction, RwTransaction, Transaction, WriteFlags,
};

use dozer_schema::registry::context::Context as SchemaContext;
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
    indexer_db: Database,
    schema_db: Database,
}

fn _debug_dump(cursor: RoCursor) -> anyhow::Result<()> {
    loop {
        if let Ok((key, val)) = cursor.get(None, None, 8) {
            println!("key: {:?}, val: {:?}", key.unwrap(), val);
        } else {
            break;
        };
    }
    Ok(())
}
async fn _get_schema_from_registry(
    client: Arc<SchemaRegistryClient>,
    schema_identifier: SchemaIdentifier,
) -> anyhow::Result<Schema> {
    let ctx = SchemaContext::current();
    let schema = client.get(ctx, schema_identifier).await?;
    Ok(schema)
}

impl LmdbCache {
    pub fn new(temp_storage: bool) -> Self {
        let env = utils::init_env(temp_storage).unwrap();
        let db = utils::init_db(&env, Some("records")).unwrap();
        let indexer_db = utils::init_db(&env, Some("indexes")).unwrap();
        let schema_db = utils::init_db(&env, Some("schemas")).unwrap();
        Self {
            env,
            db,
            indexer_db,
            schema_db,
        }
    }

    fn _insert(
        &self,
        txn: &mut RwTransaction,
        rec: &Record,
        schema: &Schema,
    ) -> anyhow::Result<()> {
        let p_key = &schema.primary_index;
        let values = &rec.values;
        let key = get_primary_key(p_key, &values);
        let encoded: Vec<u8> = bincode::serialize(&rec).unwrap();

        txn.put::<Vec<u8>, Vec<u8>>(self.db, &key, &encoded, WriteFlags::default())?;

        let indexer = Indexer::new(&self.indexer_db);

        indexer.build_indexes(txn, &rec, &schema, key)?;

        Ok(())
    }

    fn _insert_schema(
        &self,
        txn: &mut RwTransaction,
        schema: &Schema,
        name: &str,
    ) -> anyhow::Result<()> {
        let encoded: Vec<u8> = bincode::serialize(&schema)?;
        let schema_id = schema.to_owned().identifier.context("schema_id expected")?;
        let key = get_schema_key(&schema_id);
        txn.put::<Vec<u8>, Vec<u8>>(self.schema_db, &key, &encoded, WriteFlags::default())?;

        let schema_bytes = bincode::serialize(&schema_id)?;
        let schema_key = get_schema_reverse_key(name);

        txn.put::<Vec<u8>, Vec<u8>>(
            self.schema_db,
            &schema_key,
            &schema_bytes,
            WriteFlags::default(),
        )?;

        Ok(())
    }

    pub fn _debug_dump(&self) -> anyhow::Result<()> {
        let txn: RoTransaction = self.env.begin_ro_txn().unwrap();

        println!("Records:");
        _debug_dump(txn.open_ro_cursor(self.db)?)?;

        println!("Indexes:");
        _debug_dump(txn.open_ro_cursor(self.indexer_db)?)?;

        println!("Schemas:");
        _debug_dump(txn.open_ro_cursor(self.schema_db)?)?;
        Ok(())
    }

    fn _get_schema_from_reverse_key(
        &self,
        name: &str,
        txn: &RoTransaction,
    ) -> anyhow::Result<Schema> {
        let schema_reverse_key = get_schema_reverse_key(name);
        let schema_identifier = txn.get(self.schema_db, &schema_reverse_key)?;
        let schema_id: SchemaIdentifier = bincode::deserialize(schema_identifier)?;
        let schema = self._get_schema(txn, &schema_id)?;

        Ok(schema)
    }

    fn _get_schema(
        &self,
        txn: &RoTransaction,
        schema_identifier: &SchemaIdentifier,
    ) -> anyhow::Result<Schema> {
        let key = get_schema_key(schema_identifier);
        let schema = txn.get(self.schema_db, &key)?;
        let schema: Schema = bincode::deserialize(schema)?;
        Ok(schema)
    }
}

impl Cache for LmdbCache {
    fn insert_with_schema(&self, rec: &Record, schema: &Schema, name: &str) -> anyhow::Result<()> {
        if rec.schema_id != schema.identifier {
            bail!("record and schema dont have the same id.");
        }
        match schema.identifier.to_owned() {
            Some(id) => id,
            None => bail!("cache::Insert - Schema Id is not present"),
        };
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;

        self._insert_schema(&mut txn, schema, name)?;

        self._insert(&mut txn, rec, schema)?;
        txn.commit()?;
        Ok(())
    }

    fn insert(&self, rec: &Record) -> anyhow::Result<()> {
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        let schema_identifier = match rec.schema_id.to_owned() {
            Some(id) => id,
            None => bail!("cache::Insert - Schema Id is not present"),
        };
        let schema = match self.get_schema(&schema_identifier) {
            Ok(schema) => schema,
            Err(_) => {
                bail!("schema not present");
            }
        };

        self._insert(&mut txn, rec, &schema)?;
        txn.commit()?;
        Ok(())
    }

    fn delete(&self, key: &Vec<u8>) -> anyhow::Result<()> {
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        txn.del(self.db, &key, None)?;
        txn.commit()?;
        Ok(())
    }

    fn get(&self, key: &Vec<u8>) -> anyhow::Result<Record> {
        let txn: RoTransaction = self.env.begin_ro_txn()?;
        let handler = QueryHandler::new(&self.db, &self.indexer_db, &txn);
        let rec: Record = handler.get(key, &txn)?;
        Ok(rec)
    }

    fn query(
        &self,
        name: &str,
        exp: &Expression,
        no_of_rows: usize,
    ) -> anyhow::Result<Vec<Record>> {
        let txn: RoTransaction = self.env.begin_ro_txn()?;
        let schema = self._get_schema_from_reverse_key(name, &txn)?;
        let handler = QueryHandler::new(&self.db, &self.indexer_db, &txn);
        let records = handler.query(&schema, exp, no_of_rows)?;
        Ok(records)
    }

    fn update(&self, key: &Vec<u8>, rec: &Record, schema: &Schema) -> anyhow::Result<()> {
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        txn.del(self.db, &key, None)?;

        self._insert(&mut txn, rec, schema)?;
        txn.commit()?;
        Ok(())
    }

    fn get_schema_by_name(&self, name: &str) -> anyhow::Result<Schema> {
        let txn: RoTransaction = self.env.begin_ro_txn()?;
        let schema = self._get_schema_from_reverse_key(name, &txn)?;
        Ok(schema)
    }

    fn get_schema(&self, schema_identifier: &SchemaIdentifier) -> anyhow::Result<Schema> {
        let txn: RoTransaction = self.env.begin_ro_txn()?;
        self._get_schema(&txn, schema_identifier)
    }
    fn insert_schema(&self, schema: &Schema, name: &str) -> anyhow::Result<()> {
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        self._insert_schema(&mut txn, schema, name)?;
        txn.commit()?;
        Ok(())
    }
}

fn get_schema_reverse_key(name: &str) -> Vec<u8> {
    ["schema_name_".as_bytes(), name.as_bytes()].join("#".as_bytes())
}
