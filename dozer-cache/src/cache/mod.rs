pub mod lmdb;
use async_trait::async_trait;
use dozer_types::types::{Field, Operation, Record, Schema, SchemaIdentifier};

use self::expression::Expression;
pub mod expression;

#[async_trait]
pub trait Cache {
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
}

pub fn get_primary_key(primary_index: Vec<usize>, values: Vec<Field>) -> Vec<u8> {
    let key: Vec<Vec<u8>> = primary_index
        .iter()
        .map(|idx| {
            let field = values[*idx].clone();
            let encoded: Vec<u8> = bincode::serialize(&field).unwrap();
            encoded
        })
        .collect();

    key.join("#".as_bytes())
}

pub fn get_secondary_index(schema_id: u32, field_idx: &usize, field_val: &Vec<u8>) -> Vec<u8> {
    [
        "index_".as_bytes().to_vec(),
        schema_id.to_be_bytes().to_vec(),
        field_idx.to_be_bytes().to_vec(),
        field_val.to_vec(),
    ]
    .join("#".as_bytes())
}
