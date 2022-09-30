pub mod lmdb;
use async_trait::async_trait;
use dozer_types::types::{Field, Operation, Record, Schema, SchemaIdentifier};

use self::expression::Expression;
pub mod expression;

#[async_trait]
pub trait Cache: Sync {
    async fn insert(&self, rec: Record) -> anyhow::Result<()>;
    async fn delete(&self, key: Vec<u8>) -> anyhow::Result<()>;
    async fn get(&self, key: Vec<u8>) -> anyhow::Result<Record>;
    async fn query(
        &self,
        schema_identifier: SchemaIdentifier,
        exp: Expression,
    ) -> anyhow::Result<Vec<Record>>;
    async fn handle_batch(&self, operations: Vec<Operation>) -> anyhow::Result<()>;
    async fn get_schema(&self, schema_identifier: SchemaIdentifier) -> anyhow::Result<Schema>;
    async fn insert_schema(&self, schema: Schema) -> anyhow::Result<()>;
    fn get_key(&self, primary_index: Vec<usize>, values: Vec<Field>) -> Vec<u8>;
}
