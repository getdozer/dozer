use std::path::PathBuf;

use dozer_types::thiserror::Error;
use dozer_types::{bincode, serde_json, thiserror, tonic};

#[derive(Error, Debug)]
pub enum ReaderError {
    #[error("Tonic transport error: {0}")]
    TonicTransport(#[from] tonic::transport::Error),
    #[error("Tonic status: {0}")]
    TonicStatus(#[from] tonic::Status),
    #[error("Unexpected end of stream")]
    UnexpectedEndOfStream,
    #[error("Failed to deserialize log response: {0}")]
    DeserializeLogResponse(#[source] bincode::Error),
    #[error("Failed to deserialize log entry: {0}")]
    DeserializeLogEntry(#[source] bincode::Error),
    #[error("Storage error: {0}")]
    Storage(#[from] crate::storage::Error),
}

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("Filesystem error: {0:?} - {1}")]
    Filesystem(PathBuf, #[source] std::io::Error),
    #[error("Error deserializing schema: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Error)]
pub enum WriterError {
    #[error("Filesystem error: {0:?} - {1}")]
    FileSystem(PathBuf, #[source] std::io::Error),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}
