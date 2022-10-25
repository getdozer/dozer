use dozer_types::bincode;
use dozer_types::errors::cache::{CacheError, QueryError};
use lmdb::{
    Database, Environment, Error as LmdbError, RoTransaction, RwTransaction, Transaction,
    WriteFlags,
};

use dozer_schema::storage::get_schema_key;
use dozer_types::types::Record;
use dozer_types::types::{Schema, SchemaIdentifier};

use super::super::Cache;
use super::indexer::Indexer;
use super::query::handler::LmdbQueryHandler;
use super::query::helper;
use super::utils;
use crate::cache::expression::QueryExpression;
use crate::cache::index;

pub struct LmdbCache {
    env: Environment,
    db: Database,
    indexer_db: Database,
    schema_db: Database,
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
    ) -> Result<(), CacheError> {
        let p_key = &schema.primary_index;
        let values = &rec.values;
        let key = index::get_primary_key(p_key, values);
        let encoded: Vec<u8> =
            bincode::serialize(&rec).map_err(CacheError::map_serialization_error)?;

        txn.put::<Vec<u8>, Vec<u8>>(self.db, &key, &encoded, WriteFlags::empty())
            .map_err(|_e| CacheError::QueryError(QueryError::InsertValue))?;

        let indexer = Indexer::new(self.indexer_db);

        indexer.build_indexes(txn, rec, schema, key)?;

        Ok(())
    }

    fn _insert_schema(
        &self,
        txn: &mut RwTransaction,
        schema: &Schema,
        name: &str,
    ) -> Result<(), CacheError> {
        let encoded: Vec<u8> =
            bincode::serialize(&schema).map_err(CacheError::map_serialization_error)?;
        let schema_id = schema.to_owned().identifier.unwrap();
        let key = get_schema_key(&schema_id);
        txn.put::<Vec<u8>, Vec<u8>>(self.schema_db, &key, &encoded, WriteFlags::empty())
            .map_err(|_e| CacheError::QueryError(QueryError::InsertValue))?;

        let schema_bytes =
            bincode::serialize(&schema_id).map_err(CacheError::map_serialization_error)?;
        let schema_key = index::get_schema_reverse_key(name);

        txn.put::<Vec<u8>, Vec<u8>>(
            self.schema_db,
            &schema_key,
            &schema_bytes,
            WriteFlags::empty(),
        )
        .map_err(|_e| CacheError::QueryError(QueryError::InsertValue))?;

        Ok(())
    }

    pub fn get_index_db(&self) -> (&Environment, &Database) {
        (&self.env, &self.indexer_db)
    }
    pub fn get_db(&self) -> (&Environment, &Database) {
        (&self.env, &self.db)
    }
    pub fn get_schema_db(&self) -> (&Environment, &Database) {
        (&self.env, &self.schema_db)
    }

    fn _get_schema_from_reverse_key(
        &self,
        name: &str,
        txn: &RoTransaction,
    ) -> Result<Schema, CacheError> {
        let schema_reverse_key = index::get_schema_reverse_key(name);
        let schema_identifier = txn
            .get(self.schema_db, &schema_reverse_key)
            .map_err(|_e| CacheError::QueryError(QueryError::GetValue))?;
        let schema_id: SchemaIdentifier = bincode::deserialize(schema_identifier)
            .map_err(CacheError::map_deserialization_error)?;

        let schema = self._get_schema(txn, &schema_id)?;

        Ok(schema)
    }

    fn _get_schema(
        &self,
        txn: &RoTransaction,
        schema_identifier: &SchemaIdentifier,
    ) -> Result<Schema, CacheError> {
        let key = get_schema_key(schema_identifier);
        let schema = txn
            .get(self.schema_db, &key)
            .map_err(|_e| CacheError::QueryError(QueryError::GetValue))?;
        let schema: Schema =
            bincode::deserialize(schema).map_err(CacheError::map_deserialization_error)?;
        Ok(schema)
    }

    fn _delete(&self, key: &[u8]) -> Result<(), LmdbError> {
        let mut txn: RwTransaction = self.env.begin_rw_txn()?;
        txn.del(self.db, &key, None)?;
        txn.commit()?;
        Ok(())
    }
}

impl Cache for LmdbCache {
    fn insert(&self, rec: &Record) -> Result<(), CacheError> {
        let mut txn: RwTransaction = self
            .env
            .begin_rw_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let schema_identifier = rec
            .schema_id
            .to_owned()
            .map_or(Err(CacheError::SchemaIdentifierNotFound), Ok)?;
        let schema = self.get_schema(&schema_identifier)?;

        self._insert(&mut txn, rec, &schema)?;

        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))
    }

    fn delete(&self, key: &[u8]) -> Result<(), CacheError> {
        self._delete(key)
            .map_err(|e| CacheError::InternalError(Box::new(e)))
    }

    fn get(&self, key: &[u8]) -> Result<Record, CacheError> {
        let txn: RoTransaction = self
            .env
            .begin_ro_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let rec: Record = helper::get(&txn, self.db, key)?;
        Ok(rec)
    }

    fn query(&self, name: &str, query: &QueryExpression) -> Result<Vec<Record>, CacheError> {
        let txn: RoTransaction = self
            .env
            .begin_ro_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let schema = self._get_schema_from_reverse_key(name, &txn)?;

        let handler = LmdbQueryHandler::new(self.db, self.indexer_db, &txn);
        let records = handler.query(&schema, query)?;
        Ok(records)
    }

    fn update(&self, key: &[u8], rec: &Record, schema: &Schema) -> Result<(), CacheError> {
        let mut txn: RwTransaction = self
            .env
            .begin_rw_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        txn.del(self.db, &key, None)
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;

        self._insert(&mut txn, rec, schema)?;
        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }

    fn get_schema_by_name(&self, name: &str) -> Result<Schema, CacheError> {
        let txn: RoTransaction = self
            .env
            .begin_ro_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let schema = self._get_schema_from_reverse_key(name, &txn)?;
        Ok(schema)
    }

    fn get_schema(&self, schema_identifier: &SchemaIdentifier) -> Result<Schema, CacheError> {
        let txn: RoTransaction = self
            .env
            .begin_ro_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        self._get_schema(&txn, schema_identifier)
    }
    fn insert_schema(&self, name: &str, schema: &Schema) -> Result<(), CacheError> {
        let mut txn: RwTransaction = self
            .env
            .begin_rw_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        self._insert_schema(&mut txn, schema, name)?;
        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }
}
