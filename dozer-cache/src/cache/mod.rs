mod lmdb;
mod redis;
use std::any::Any;

use self::expression::QueryExpression;
pub use self::lmdb::{
    cache::lmdb as lmdb_rs, cache::LmdbCache, CacheCommonOptions, CacheOptions, CacheOptionsKind,
    CacheReadOptions, CacheWriteOptions,
};
use crate::errors::CacheError;
use dozer_types::types::{IndexDefinition, Record, Schema, SchemaIdentifier};
pub mod expression;
pub mod index;
mod plan;
pub mod test_utils;
pub trait Cache {
    fn begin_rw_txn(&self) -> Result<Box<dyn CacheTransaction>, CacheError>;
    // Schema Operations
    fn insert_schema(
        &self,
        name: &str,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
    ) -> Result<(), CacheError>;
    fn get_schema(&self, schema_identifier: &SchemaIdentifier) -> Result<Schema, CacheError>;
    fn get_schema_and_indexes_by_name(
        &self,
        name: &str,
    ) -> Result<(Schema, Vec<IndexDefinition>), CacheError>;

    // With Txn Operations
    fn insert_with_txn(
        &self,
        txn: Box<dyn CacheTransaction>,
        record: &Record,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
    ) -> Result<(), CacheError>;

    fn delete_with_txn(
        &self,
        txn: Box<dyn CacheTransaction>,
        key: &[u8],
        record: &Record,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
    ) -> Result<(), CacheError>;
    fn update_with_txn(
        &self,
        txn: Box<dyn CacheTransaction>,
        key: &[u8],
        old: &Record,
        new: &Record,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
    ) -> Result<(), CacheError>;

    // Record Operations
    fn insert(&self, record: &Record) -> Result<(), CacheError>;
    fn delete(&self, key: &[u8]) -> Result<(), CacheError>;
    fn update(&self, key: &[u8], record: &Record) -> Result<(), CacheError>;
    fn get(&self, key: &[u8]) -> Result<Record, CacheError>;
    fn count(&self, schema_name: &str, query: &QueryExpression) -> Result<usize, CacheError>;
    fn query(&self, schema_name: &str, query: &QueryExpression) -> Result<Vec<Record>, CacheError>;
}

pub trait CacheTransaction {
    fn as_any(&self) -> &dyn Any;
}
