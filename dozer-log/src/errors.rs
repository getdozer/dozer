use std::path::PathBuf;

use dozer_types::errors::internal::BoxedError;
use dozer_types::thiserror::Error;
use dozer_types::{bincode, thiserror};

#[derive(Error, Debug)]
pub enum ReaderError {
    #[error("Cannot find log file {0:?}")]
    LogFileNotFound(PathBuf),
    #[error("Cannot read log {0:?}")]
    LogReadError(#[source] std::io::Error),
    #[error("Error reading log: {0}")]
    ReadError(#[source] std::io::Error),
    #[error("Error seeking file log: {0},pos: {1}, error: {2}")]
    SeekError(String, u64, #[source] std::io::Error),
    #[error("Error deserializing log: {0}")]
    DeserializationError(#[from] bincode::Error),
}

#[derive(Error, Debug)]
pub enum SchemasError {
    #[error("Schemas not found in Path specified {0:?}")]
    SchemasNotInitializedPath(PathBuf),
    #[error("Cannot convert Schemas in Path specified {0:?}")]
    DeserializeSchemas(PathBuf),
    #[error(transparent)]
    InternalError(#[from] BoxedError),
    #[error("Got mismatching primary key for `{endpoint_name}`. Expected: `{expected:?}`, got: `{actual:?}`")]
    MismatchPrimaryKey {
        endpoint_name: String,
        expected: Vec<String>,
        actual: Vec<String>,
    },
    #[error("Field not found at position {0}")]
    FieldNotFound(String),
}
