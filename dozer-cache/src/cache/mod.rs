pub mod lmdb;
use async_trait::async_trait;
use dozer_types::types::{Field, Record, Schema, SchemaIdentifier};

use self::expression::Expression;
pub mod expression;

#[async_trait]
pub trait Cache {
    fn insert_with_schema(&self, rec: &Record, schema: &Schema, name: &str) -> anyhow::Result<()>;
    fn insert(&self, rec: &Record) -> anyhow::Result<()>;
    fn delete(&self, key: &[u8]) -> anyhow::Result<()>;
    fn update(&self, key: &[u8], rec: &Record, schema: &Schema) -> anyhow::Result<()>;
    fn get(&self, key: &[u8]) -> anyhow::Result<Record>;
    fn query(
        &self,
        schema_name: &str,
        exp: &Expression,
        no_of_rows: Option<usize>,
    ) -> anyhow::Result<Vec<Record>>;
    fn get_schema_by_name(&self, name: &str) -> anyhow::Result<Schema>;
    fn get_schema(&self, schema_identifier: &SchemaIdentifier) -> anyhow::Result<Schema>;
    fn insert_schema(&self, schema: &Schema, name: &str) -> anyhow::Result<()>;
}

pub fn get_primary_key(primary_index: &[usize], values: &[Field]) -> Vec<u8> {
    let key: Vec<Vec<u8>> = primary_index
        .iter()
        .map(|idx| {
            let field = &values[*idx];
            let encoded: Vec<u8> = bincode::serialize(field).unwrap();
            encoded
        })
        .collect();

    key.join("#".as_bytes())
}

pub fn get_secondary_index(schema_id: u32, field_idx: &usize, field_val: &[u8]) -> Vec<u8> {
    [
        "index_".as_bytes().to_vec(),
        schema_id.to_be_bytes().to_vec(),
        field_idx.to_be_bytes().to_vec(),
        field_val.to_vec(),
    ]
    .join("#".as_bytes())
}
