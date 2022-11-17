mod lmdb;
use self::expression::QueryExpression;
pub use self::lmdb::{cache::LmdbCache, CacheOptions, CacheReadOptions, CacheWriteOptions};
use crate::errors::CacheError;
use dozer_types::types::{Record, Schema, SchemaIdentifier};
pub mod expression;
pub mod index;
mod plan;
pub mod test_utils;
pub trait Cache {
    // Schema Operations
    fn insert_schema(&self, name: &str, schema: &Schema) -> Result<(), CacheError>;
    fn get_schema(&self, schema_identifier: &SchemaIdentifier) -> Result<Schema, CacheError>;
    fn get_schema_by_name(&self, name: &str) -> Result<Schema, CacheError>;

    // Record Operations
    fn insert(&self, rec: &Record) -> Result<(), CacheError>;
    fn delete(&self, key: &[u8]) -> Result<(), CacheError>;
    fn update(&self, key: &[u8], rec: &Record, schema: &Schema) -> Result<(), CacheError>;
    fn get(&self, key: &[u8]) -> Result<Record, CacheError>;
    fn query(&self, schema_name: &str, query: &QueryExpression) -> Result<Vec<Record>, CacheError>;
}
