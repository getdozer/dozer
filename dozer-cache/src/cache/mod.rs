pub mod lmdb;
use self::expression::QueryExpression;
use dozer_types::types::{Record, Schema, SchemaIdentifier};
pub mod expression;
pub mod index;
pub mod planner;
pub mod test_utils;

pub trait Cache {
    fn insert_with_schema(&self, rec: &Record, schema: &Schema, name: &str) -> anyhow::Result<()>;
    fn insert(&self, rec: &Record) -> anyhow::Result<()>;
    fn delete(&self, key: &[u8]) -> anyhow::Result<()>;
    fn update(&self, key: &[u8], rec: &Record, schema: &Schema) -> anyhow::Result<()>;
    fn get(&self, key: &[u8]) -> anyhow::Result<Record>;
    fn query(&self, schema_name: &str, query: &QueryExpression) -> anyhow::Result<Vec<Record>>;
    fn get_schema_by_name(&self, name: &str) -> anyhow::Result<Schema>;
    fn get_schema(&self, schema_identifier: &SchemaIdentifier) -> anyhow::Result<Schema>;
    fn insert_schema(&self, schema: &Schema, name: &str) -> anyhow::Result<()>;
}
