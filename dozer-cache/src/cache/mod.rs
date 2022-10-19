mod lmdb;
use self::expression::QueryExpression;
pub use self::lmdb::cache::LmdbCache;
use dozer_types::types::{Record, Schema, SchemaIdentifier};
pub mod expression;
pub mod index;
mod planner;
pub mod test_utils;
pub trait Cache {
    // Schema Operations
    fn insert_schema(&self, name: &str, schema: &Schema) -> anyhow::Result<()>;
    fn get_schema(&self, schema_identifier: &SchemaIdentifier) -> anyhow::Result<Schema>;
    fn get_schema_by_name(&self, name: &str) -> anyhow::Result<Schema>;

    // Record Operations
    fn insert(&self, rec: &Record) -> anyhow::Result<()>;
    fn delete(&self, key: &[u8]) -> anyhow::Result<()>;
    fn update(&self, key: &[u8], rec: &Record, schema: &Schema) -> anyhow::Result<()>;
    fn get(&self, key: &[u8]) -> anyhow::Result<Record>;
    fn query(&self, schema_name: &str, query: &QueryExpression) -> anyhow::Result<Vec<Record>>;
}
