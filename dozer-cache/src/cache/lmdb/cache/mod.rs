use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use dozer_storage::lmdb::{RoTransaction, RwTransaction, Transaction};
use dozer_storage::lmdb_storage::{
    LmdbEnvironmentManager, LmdbExclusiveTransaction, SharedTransaction,
};
use dozer_types::log::info;
use dozer_types::parking_lot::{RwLock, RwLockReadGuard};

use dozer_types::types::{Field, FieldType, IndexDefinition, Record};
use dozer_types::types::{Schema, SchemaIdentifier};

use super::super::{RoCache, RwCache};
use super::indexer::Indexer;
use super::query::handler::LmdbQueryHandler;
use super::{
    utils, CacheCommonOptions, CacheOptions, CacheOptionsKind, CacheReadOptions, CacheWriteOptions,
};
use crate::cache::expression::QueryExpression;
use crate::cache::index::get_primary_key;
use crate::errors::CacheError;

mod id_database;
mod record_database;
mod schema_database;
mod secondary_index_database;

pub use id_database::IdDatabase;
pub use record_database::RecordDatabase;
use schema_database::SchemaDatabase;
use secondary_index_database::SecondaryIndexDatabase;

pub type SecondaryIndexDatabases = HashMap<(SchemaIdentifier, usize), SecondaryIndexDatabase>;

#[derive(Debug)]
pub struct LmdbRoCache {
    common: LmdbCacheCommon,
    env: LmdbEnvironmentManager,
}

impl LmdbRoCache {
    pub fn new(options: CacheCommonOptions) -> Result<Self, CacheError> {
        let mut env = utils::init_env(&CacheOptions {
            common: options.clone(),
            kind: CacheOptionsKind::ReadOnly(CacheReadOptions {}),
        })?;
        let common = LmdbCacheCommon::new(&mut env, options, true)?;
        Ok(Self { common, env })
    }
}

#[derive(Debug)]
pub struct LmdbRwCache {
    common: LmdbCacheCommon,
    txn: SharedTransaction,
}

impl LmdbRwCache {
    pub fn new(
        common_options: CacheCommonOptions,
        write_options: CacheWriteOptions,
    ) -> Result<Self, CacheError> {
        let mut env = utils::init_env(&CacheOptions {
            common: common_options.clone(),
            kind: CacheOptionsKind::Write(write_options),
        })?;
        let common = LmdbCacheCommon::new(&mut env, common_options, false)?;
        let txn = env.create_txn()?;
        Ok(Self { common, txn })
    }
}

impl<C: LmdbCache> RoCache for C {
    fn get(&self, key: &[u8]) -> Result<Record, CacheError> {
        let txn = self.begin_txn()?;
        let txn = txn.as_txn();
        self.common().db.get(txn, self.common().id.get(txn, key)?)
    }

    fn count(&self, schema_name: &str, query: &QueryExpression) -> Result<usize, CacheError> {
        let txn = self.begin_txn()?;
        let txn = txn.as_txn();
        let handler = self.create_query_handler(txn, schema_name, query)?;
        handler.count()
    }

    fn query(&self, schema_name: &str, query: &QueryExpression) -> Result<Vec<Record>, CacheError> {
        let txn = self.begin_txn()?;
        let txn = txn.as_txn();
        let handler = self.create_query_handler(txn, schema_name, query)?;
        handler.query()
    }

    fn get_schema_and_indexes_by_name(
        &self,
        name: &str,
    ) -> Result<(Schema, Vec<IndexDefinition>), CacheError> {
        let txn = self.begin_txn()?;
        let txn = txn.as_txn();
        let schema = self.common().schema_db.get_schema_from_name(txn, name)?;
        Ok(schema)
    }

    fn get_schema(&self, schema_identifier: &SchemaIdentifier) -> Result<Schema, CacheError> {
        let txn = self.begin_txn()?;
        let txn = txn.as_txn();
        self.common()
            .schema_db
            .get_schema(txn, *schema_identifier)
            .map(|(schema, _)| schema)
    }
}

impl RwCache for LmdbRwCache {
    fn insert(&self, record: &Record) -> Result<(), CacheError> {
        let (schema, secondary_indexes) = self.get_schema_and_indexes_from_record(record)?;

        let mut txn = self.txn.write();
        let txn = txn.txn_mut();

        let id = if schema.primary_index.is_empty() {
            self.common.id.get_or_generate(txn, None)?
        } else {
            let primary_key = get_primary_key(&schema.primary_index, &record.values);
            self.common.id.get_or_generate(txn, Some(&primary_key))?
        };
        self.common.db.insert(txn, id, record)?;

        let indexer = Indexer {
            secondary_indexes: self.common.secondary_indexes.clone(),
        };

        indexer.build_indexes(txn, record, &schema, &secondary_indexes, id)
    }

    fn delete(&self, key: &[u8]) -> Result<(), CacheError> {
        let record = self.get(key)?;
        let (schema, secondary_indexes) = self.get_schema_and_indexes_from_record(&record)?;

        let mut txn = self.txn.write();
        let txn = txn.txn_mut();

        let id = self.common.id.get(txn, key)?;
        self.common.db.delete(txn, id)?;

        let indexer = Indexer {
            secondary_indexes: self.common.secondary_indexes.clone(),
        };
        indexer.delete_indexes(txn, &record, &schema, &secondary_indexes, id)
    }

    fn update(&self, key: &[u8], record: &Record) -> Result<(), CacheError> {
        self.delete(key)?;
        self.insert(record)
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
        let mut txn = self.txn.write();
        for (idx, index) in secondary_indexes.iter().enumerate() {
            let db = SecondaryIndexDatabase::create(&mut txn, &schema_id, idx, index, true)?;
            self.common
                .secondary_indexes
                .write()
                .insert((schema_id, idx), db);
        }

        self.common
            .schema_db
            .insert(txn.txn_mut(), name, schema, secondary_indexes)?;

        txn.commit_and_renew()?;
        Ok(())
    }

    fn commit(&self) -> Result<(), CacheError> {
        self.txn.write().commit_and_renew()?;
        Ok(())
    }
}

/// This trait abstracts the behavior of getting a transaction from a `LmdbExclusiveTransaction` or a `lmdb::Transaction`.
trait AsTransaction {
    type Transaction<'a>: Transaction
    where
        Self: 'a;

    fn as_txn(&self) -> &Self::Transaction<'_>;
}

impl<'a> AsTransaction for RoTransaction<'a> {
    type Transaction<'env> = RoTransaction<'env> where Self: 'env;

    fn as_txn(&self) -> &Self::Transaction<'_> {
        self
    }
}

impl<'a> AsTransaction for RwLockReadGuard<'a, LmdbExclusiveTransaction> {
    type Transaction<'env> = RwTransaction<'env> where Self: 'env;

    fn as_txn(&self) -> &Self::Transaction<'_> {
        self.txn()
    }
}

/// This trait abstracts the behavior of locking a `SharedTransaction` for reading
/// and beginning a `RoTransaction` from `LmdbEnvironmentManager`.
trait LmdbCache: Send + Sync + Debug {
    type AsTransaction<'a>: AsTransaction
    where
        Self: 'a;

    fn common(&self) -> &LmdbCacheCommon;
    fn begin_txn(&self) -> Result<Self::AsTransaction<'_>, CacheError>;

    fn create_query_handler<'a, T: Transaction>(
        &'a self,
        txn: &'a T,
        schema_name: &str,
        query: &'a QueryExpression,
    ) -> Result<LmdbQueryHandler<'a, T>, CacheError> {
        let (schema, secondary_indexes) = self
            .common()
            .schema_db
            .get_schema_from_name(txn, schema_name)?;

        Ok(LmdbQueryHandler::new(
            self.common().db,
            self.common().secondary_indexes.clone(),
            txn,
            schema,
            secondary_indexes,
            query,
            self.common().cache_options.intersection_chunk_size,
        ))
    }

    fn get_schema_and_indexes_from_record(
        &self,
        record: &Record,
    ) -> Result<(Schema, Vec<IndexDefinition>), CacheError> {
        let schema_identifier = record
            .schema_id
            .ok_or(CacheError::SchemaIdentifierNotFound)?;
        let (schema, secondary_indexes) = self
            .common()
            .schema_db
            .get_schema(self.begin_txn()?.as_txn(), schema_identifier)?;

        debug_check_schema_record_consistency(&schema, record);

        Ok((schema, secondary_indexes))
    }
}

impl LmdbCache for LmdbRoCache {
    type AsTransaction<'a> = RoTransaction<'a>;

    fn common(&self) -> &LmdbCacheCommon {
        &self.common
    }

    fn begin_txn(&self) -> Result<Self::AsTransaction<'_>, CacheError> {
        Ok(self.env.begin_ro_txn()?)
    }
}

impl LmdbCache for LmdbRwCache {
    type AsTransaction<'a> = RwLockReadGuard<'a, LmdbExclusiveTransaction>;

    fn common(&self) -> &LmdbCacheCommon {
        &self.common
    }

    fn begin_txn(&self) -> Result<Self::AsTransaction<'_>, CacheError> {
        Ok(self.txn.read())
    }
}

fn debug_check_schema_record_consistency(schema: &Schema, record: &Record) {
    debug_assert_eq!(schema.identifier, record.schema_id);
    debug_assert_eq!(schema.fields.len(), record.values.len());
    for (field, value) in schema.fields.iter().zip(record.values.iter()) {
        if field.nullable && value == &Field::Null {
            continue;
        }
        match field.typ {
            FieldType::UInt => {
                info!("UInt {:?}", value);
                info!("UInt {:?}", field);
                debug_assert!(value.as_uint().is_some())
            },
            FieldType::Int => {
                info!("Int {:?}", value);
                info!("Int {:?}", field);
                debug_assert!(value.as_int().is_some())
            },
            FieldType::Float => {
                info!("Float {:?}", value);
                info!("Float {:?}", field);
                debug_assert!(value.as_float().is_some())
            },
            FieldType::Boolean => debug_assert!(value.as_boolean().is_some()),
            FieldType::String => debug_assert!(value.as_string().is_some()),
            FieldType::Text => debug_assert!(value.as_text().is_some()),
            FieldType::Binary => debug_assert!(value.as_binary().is_some()),
            FieldType::Decimal => debug_assert!(value.as_decimal().is_some()),
            FieldType::Timestamp => debug_assert!(value.as_timestamp().is_some()),
            FieldType::Date => debug_assert!(value.as_date().is_some()),
            FieldType::Bson => debug_assert!(value.as_bson().is_some()),
        }
    }
}

#[derive(Debug)]
pub struct LmdbCacheCommon {
    db: RecordDatabase,
    id: IdDatabase,
    secondary_indexes: Arc<RwLock<SecondaryIndexDatabases>>,
    schema_db: SchemaDatabase,
    cache_options: CacheCommonOptions,
}

impl LmdbCacheCommon {
    fn new(
        env: &mut LmdbEnvironmentManager,
        options: CacheCommonOptions,
        read_only: bool,
    ) -> Result<Self, CacheError> {
        // Create or open must have databases.
        let db = RecordDatabase::new(env, !read_only)?;
        let id = IdDatabase::new(env, !read_only)?;
        let schema_db = SchemaDatabase::new(env, !read_only)?;

        // Open existing secondary index databases.
        let mut secondary_indexe_databases = HashMap::default();
        let schemas = schema_db.get_all_schemas(env)?;
        for (schema, secondary_indexes) in schemas {
            let schema_id = schema
                .identifier
                .ok_or(CacheError::SchemaIdentifierNotFound)?;
            for (index, index_definition) in secondary_indexes.iter().enumerate() {
                let db = SecondaryIndexDatabase::open(env, &schema_id, index, index_definition)?;
                secondary_indexe_databases.insert((schema_id, index), db);
            }
        }

        Ok(Self {
            db,
            id,
            secondary_indexes: Arc::new(RwLock::new(secondary_indexe_databases)),
            schema_db,
            cache_options: options,
        })
    }
}

/// Methods for testing.
#[cfg(test)]
mod tests {
    use super::*;

    impl LmdbRwCache {
        pub fn get_txn_and_secondary_indexes(
            &self,
        ) -> (&SharedTransaction, &RwLock<SecondaryIndexDatabases>) {
            (&self.txn, &self.common.secondary_indexes)
        }
    }
}
