use std::collections::HashMap;
use std::sync::Arc;

use dozer_types::parking_lot::RwLock;
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

mod primary_index_database;
mod record_database;
mod schema_database;

pub use primary_index_database::PrimaryIndexDatabase;
pub use record_database::RecordDatabase;
use schema_database::SchemaDatabase;

pub type SecondaryIndexDatabases = HashMap<(SchemaIdentifier, usize), Database>;

pub struct LmdbCache {
    env: Environment,
    db: RecordDatabase,
    primary_index: PrimaryIndexDatabase,
    secondary_indexes: Arc<RwLock<SecondaryIndexDatabases>>,
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
            secondary_indexes: Arc::new(RwLock::new(Default::default())),
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
            secondary_indexes: self.secondary_indexes.clone(),
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
            secondary_indexes: self.secondary_indexes.clone(),
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
            self.secondary_indexes.clone(),
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
        let schema_id = schema
            .identifier
            .ok_or(CacheError::SchemaIdentifierNotFound)?;

        // Create a db for each index
        for (idx, index) in secondary_indexes.iter().enumerate() {
            let name = format!("index_#{}_#{}_#{}", schema_id.id, schema_id.version, idx);
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

            self.secondary_indexes.write().insert((schema_id, idx), db);
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

    impl LmdbCache {
        pub fn get_env_and_secondary_indexes(
            &self,
        ) -> (&Environment, &RwLock<SecondaryIndexDatabases>) {
            (&self.env, &self.secondary_indexes)
        }
    }
}
