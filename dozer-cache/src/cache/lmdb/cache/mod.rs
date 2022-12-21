use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub use lmdb;
use lmdb::{Database, Environment, RoTransaction, RwTransaction, Transaction};

use dozer_types::types::{IndexDefinition, Record};
use dozer_types::types::{Schema, SchemaIdentifier};

use super::super::Cache;
use super::indexer::Indexer;
use super::query::handler::LmdbQueryHandler;
use super::utils::DatabaseCreateOptions;
use super::{comparator, utils, CacheOptions, CacheOptionsKind};
use crate::cache::expression::QueryExpression;
use crate::errors::CacheError;

pub struct IndexMetaData {
    //schema_id, secondary_key
    indexes: RwLock<HashMap<usize, Database>>,
}
impl Default for IndexMetaData {
    fn default() -> Self {
        Self::new()
    }
}
impl IndexMetaData {
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_key(schema: &Schema, idx: usize) -> usize {
        schema.identifier.as_ref().unwrap().id as usize * 100000 + idx
    }

    pub fn insert_index(&self, key: usize, db: Database) {
        self.indexes.write().map(|mut h| h.insert(key, db)).unwrap();
    }
    pub fn get_db(&self, schema: &Schema, idx: usize) -> Database {
        let key = Self::get_key(schema, idx);
        self.indexes.read().unwrap().get(&key).unwrap().to_owned()
    }
}

mod primary_index_database;
mod record_database;
mod schema_database;

pub use primary_index_database::PrimaryIndexDatabase;
pub use record_database::RecordDatabase;
use schema_database::SchemaDatabase;

pub struct LmdbCache {
    env: Environment,
    db: RecordDatabase,
    primary_index: PrimaryIndexDatabase,
    index_metadata: Arc<IndexMetaData>,
    schema_db: SchemaDatabase,
    cache_options: CacheOptions,
}

impl LmdbCache {
    pub fn begin_rw_txn(&self) -> Result<RwTransaction, CacheError> {
        self.env
            .begin_rw_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))
    }
    pub fn new(cache_options: CacheOptions) -> Result<Self, CacheError> {
        let env = utils::init_env(&cache_options)?;

        let create_if_not_exist = matches!(cache_options.kind, CacheOptionsKind::Write(_));
        let db = RecordDatabase::new(&env, create_if_not_exist)?;
        let primary_index = PrimaryIndexDatabase::new(&env, create_if_not_exist)?;
        let schema_db = SchemaDatabase::new(&env, create_if_not_exist)?;
        Ok(Self {
            env,
            db,
            primary_index,
            index_metadata: Arc::new(IndexMetaData::default()),
            schema_db,
            cache_options,
        })
    }

    pub fn insert_with_txn(
        &self,
        txn: &mut RwTransaction,
        record: &Record,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
    ) -> Result<(), CacheError> {
        let id = self.db.insert(txn, record)?;

        let indexer = Indexer {
            primary_index: self.primary_index,
            index_metadata: self.index_metadata.clone(),
        };

        indexer.build_indexes(txn, record, schema, secondary_indexes, id)?;

        Ok(())
    }

    pub fn get_schema_and_indexes_from_record<T: Transaction>(
        &self,
        txn: &T,
        record: &Record,
    ) -> Result<(Schema, Vec<IndexDefinition>), CacheError> {
        let schema_identifier = record
            .schema_id
            .ok_or(CacheError::SchemaIdentifierNotFound)?;
        self.schema_db.get_schema(txn, schema_identifier)
    }

    fn get_with_txn<T: Transaction>(&self, txn: &T, key: &[u8]) -> Result<Record, CacheError> {
        self.db.get(txn, self.primary_index.get(txn, key)?)
    }

    pub fn delete_with_txn(
        &self,
        txn: &mut RwTransaction,
        key: &[u8],
        record: &Record,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
    ) -> Result<(), CacheError> {
        let id = self.primary_index.get(txn, key)?;
        self.db.delete(txn, id)?;

        let indexer = Indexer {
            primary_index: self.primary_index,
            index_metadata: self.index_metadata.clone(),
        };
        indexer.delete_indexes(txn, record, schema, secondary_indexes, key, id)
    }

    pub fn update_with_txn(
        &self,
        txn: &mut RwTransaction,
        key: &[u8],
        old: &Record,
        new: &Record,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
    ) -> Result<(), CacheError> {
        self.delete_with_txn(txn, key, old, schema, secondary_indexes)?;

        self.insert_with_txn(txn, new, schema, secondary_indexes)
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }
}

impl Cache for LmdbCache {
    fn insert(&self, record: &Record) -> Result<(), CacheError> {
        let mut txn: RwTransaction = self
            .env
            .begin_rw_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let (schema, secondary_indexes) = self.get_schema_and_indexes_from_record(&txn, record)?;

        self.insert_with_txn(&mut txn, record, &schema, &secondary_indexes)?;
        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), CacheError> {
        let mut txn: RwTransaction = self
            .env
            .begin_rw_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;

        let record = self.get_with_txn(&txn, key)?;
        let (schema, secondary_indexes) = self.get_schema_and_indexes_from_record(&txn, &record)?;
        self.delete_with_txn(&mut txn, key, &record, &schema, &secondary_indexes)?;

        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Record, CacheError> {
        let txn: RoTransaction = self
            .env
            .begin_ro_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        self.get_with_txn(&txn, key)
    }

    fn query(&self, name: &str, query: &QueryExpression) -> Result<Vec<Record>, CacheError> {
        let txn: RoTransaction = self
            .env
            .begin_ro_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let (schema, secondary_indexes) = self.schema_db.get_schema_from_name(&txn, name)?;

        let handler = LmdbQueryHandler::new(
            self.db,
            self.index_metadata.clone(),
            &txn,
            &schema,
            &secondary_indexes,
            query,
            self.cache_options.common.intersection_chunk_size,
        );
        let records = handler.query()?;
        Ok(records)
    }

    fn update(&self, key: &[u8], record: &Record) -> Result<(), CacheError> {
        let mut txn: RwTransaction = self
            .env
            .begin_rw_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let old_record = self.get_with_txn(&txn, key)?;
        let (schema, secondary_indexes) =
            self.get_schema_and_indexes_from_record(&txn, &old_record)?;
        self.update_with_txn(
            &mut txn,
            key,
            &old_record,
            record,
            &schema,
            &secondary_indexes,
        )?;
        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }

    fn get_schema_and_indexes_by_name(
        &self,
        name: &str,
    ) -> Result<(Schema, Vec<IndexDefinition>), CacheError> {
        let txn: RoTransaction = self
            .env
            .begin_ro_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        let schema = self.schema_db.get_schema_from_name(&txn, name)?;
        Ok(schema)
    }

    fn get_schema(&self, schema_identifier: &SchemaIdentifier) -> Result<Schema, CacheError> {
        let txn: RoTransaction = self
            .env
            .begin_ro_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        self.schema_db
            .get_schema(&txn, *schema_identifier)
            .map(|(schema, _)| schema)
    }
    fn insert_schema(
        &self,
        name: &str,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
    ) -> Result<(), CacheError> {
        // Create a db for each index
        for (idx, index) in secondary_indexes.iter().enumerate() {
            let key = IndexMetaData::get_key(schema, idx);
            let name = format!("index_#{}", key);
            let db = utils::init_db(
                &self.env,
                Some(&name),
                Some(DatabaseCreateOptions {
                    allow_dup: true,
                    fixed_length_key: false,
                }),
            )?;

            if let IndexDefinition::SortedInverted(fields) = index {
                comparator::set_sorted_inverted_comparator(&self.env, db, fields)
                    .map_err(|e| CacheError::InternalError(Box::new(e)))?;
            }

            self.index_metadata.insert_index(key, db);
        }

        let mut txn: RwTransaction = self
            .env
            .begin_rw_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        self.schema_db
            .insert(&mut txn, name, schema, secondary_indexes)?;
        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }
}

/// Methods for testing.
#[cfg(test)]
mod tests {
    use super::*;

    impl IndexMetaData {
        pub fn get_all_raw(&self) -> HashMap<usize, Database> {
            self.indexes.read().unwrap().to_owned()
        }
    }

    impl LmdbCache {
        pub fn get_index_metadata(&self) -> (&Environment, Arc<IndexMetaData>) {
            (&self.env, self.index_metadata.clone())
        }
    }
}
