mod lmdb;
use std::fmt::Debug;

use self::expression::QueryExpression;
use crate::errors::CacheError;
use dozer_types::types::{IndexDefinition, Record, Schema, SchemaIdentifier};
pub use lmdb::{
    cache::{LmdbRoCache, LmdbRwCache},
    CacheCommonOptions, CacheOptions, CacheOptionsKind, CacheReadOptions, CacheWriteOptions,
};
pub mod expression;
pub mod index;
mod plan;
pub mod test_utils;
pub trait RoCache: Send + Sync + Debug {
    // Schema Operations
    fn get_schema(&self, schema_identifier: &SchemaIdentifier) -> Result<Schema, CacheError>;
    fn get_schema_and_indexes_by_name(
        &self,
        name: &str,
    ) -> Result<(Schema, Vec<IndexDefinition>), CacheError>;

    // Record Operations
    fn get(&self, key: &[u8]) -> Result<Record, CacheError>;
    fn count(&self, schema_name: &str, query: &QueryExpression) -> Result<usize, CacheError>;
    fn query(&self, schema_name: &str, query: &QueryExpression) -> Result<Vec<Record>, CacheError>;
}

pub trait RwCache: RoCache {
    // Schema Operations
    fn insert_schema(
        &self,
        name: &str,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
    ) -> Result<(), CacheError>;

    // Record Operations
    fn insert(&self, record: &Record) -> Result<(), CacheError>;
    fn delete(&self, key: &[u8]) -> Result<(), CacheError>;
    fn update(&self, key: &[u8], record: &Record) -> Result<(), CacheError>;
    fn commit(&self) -> Result<(), CacheError>;
}
