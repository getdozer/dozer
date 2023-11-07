use dozer_types::thiserror::Error;
use dozer_types::{bincode, serde_json, thiserror, tonic};

use crate::replication::LoadPersistedLogEntryError;

#[derive(Error, Debug)]
pub enum ReaderBuilderError {
    #[error("Tonic transport error: {0:?}")]
    TonicTransport(#[from] tonic::transport::Error),
    #[error("Tonic status: {0}")]
    TonicStatus(#[from] tonic::Status),
    #[error("Storage error: {0}")]
    Storage(#[from] crate::storage::Error),
    #[error("Deserialize schema: {0}")]
    DeserializeSchema(#[from] serde_json::Error),
}

#[derive(Debug, Error)]
pub enum ReaderError {
    #[error("Log stream ended")]
    LogStreamEnded,
    #[error("Tonic error: {0}")]
    Tonic(#[from] tonic::Status),
    #[error("Failed to deserialize log response: {0}")]
    DeserializeLogResponse(#[source] bincode::error::DecodeError),
    #[error("Failed to load persisted log entry: {0}")]
    LoadPersistedLogEntry(#[from] LoadPersistedLogEntryError),
    #[error("Reader thread has quit: {0:?}")]
    ReaderThreadQuit(#[source] Option<tokio::task::JoinError>),
}
