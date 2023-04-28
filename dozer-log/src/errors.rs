use std::path::PathBuf;

use dozer_types::thiserror::Error;
use dozer_types::{bincode, serde_json, thiserror};

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

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("Filesystem error: {0:?} - {1:?}")]
    Filesystem(PathBuf, #[source] std::io::Error),
    #[error("Error deserializing schema: {0:?}")]
    Json(#[from] serde_json::Error),
}
