use std::{fs::OpenOptions, path::Path};

use dozer_types::{
    serde::{Deserialize, Serialize},
    serde_json,
    types::{IndexDefinition, Schema},
};
use std::io::Write;

use crate::errors::SchemaError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(crate = "dozer_types::serde")]
pub struct MigrationSchema {
    pub schema: Schema,
    pub secondary_indexes: Vec<IndexDefinition>,
    pub enable_token: bool,
    pub enable_on_event: bool,
}

pub fn write_schema(schema: &MigrationSchema, schema_path: &Path) -> Result<(), SchemaError> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(schema_path)
        .map_err(|e| SchemaError::Filesystem(schema_path.to_path_buf(), e))?;
    writeln!(file, "{}", serde_json::to_string(&schema).unwrap())
        .map_err(|e| SchemaError::Filesystem(schema_path.to_path_buf(), e))?;

    Ok(())
}

pub fn load_schema(schema_path: &Path) -> Result<MigrationSchema, SchemaError> {
    let schema_str = std::fs::read_to_string(schema_path)
        .map_err(|e| SchemaError::Filesystem(schema_path.to_path_buf(), e))?;

    serde_json::from_str(&schema_str).map_err(Into::into)
}
