use std::path::PathBuf;

use crate::checkpoint::serialize::{DeserializationError, SerializationError};
use crate::node::PortHandle;
use dozer_log::reader::CheckpointedLogReaderError;
use dozer_recordstore::RecordStoreError;
use dozer_types::errors::internal::BoxedError;
use dozer_types::node::NodeHandle;
use dozer_types::thiserror::Error;
use dozer_types::{bincode, thiserror};

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("Adding this edge would have created a cycle")]
    WouldCycle,
    #[error("Invalid port handle: {0}")]
    InvalidPortHandle(PortHandle),
    #[error("Missing input for node {node} on port {port}")]
    MissingInput { node: NodeHandle, port: PortHandle },
    #[error("Duplicate input for node {node} on port {port}")]
    DuplicateInput { node: NodeHandle, port: PortHandle },
    #[error("Cannot send to channel")]
    CannotSendToChannel,
    #[error("Cannot receive from channel")]
    CannotReceiveFromChannel,
    #[error("Cannot spawn worker thread: {0}")]
    CannotSpawnWorkerThread(#[source] std::io::Error),
    #[error("Invalid source name {0}")]
    InvalidSourceIdentifier(String),
    #[error("Ambiguous source name {0}")]
    AmbiguousSourceIdentifier(String),
    #[error("Invalid AppSource connection {0}. Already exists.")]
    AppSourceConnectionAlreadyExists(String),
    #[error("Factory error: {0}")]
    Factory(#[source] BoxedError),
    #[error("Failed to restore record writer: {0}")]
    RestoreRecordWriter(#[source] DeserializationError),
    #[error("Source error: {0}")]
    Source(#[source] BoxedError),
    #[error("Sink error: {0}")]
    Sink(#[source] BoxedError),
    #[error("State of {0} is not consistent across sinks")]
    SourceStateConflict(NodeHandle),
    #[error("File system error {0:?}: {1}")]
    FileSystemError(PathBuf, #[source] std::io::Error),
    #[error("Recordstore error: {0}")]
    RecordStore(#[from] RecordStoreError),
    #[error("Object storage error: {0}")]
    ObjectStorage(#[from] dozer_log::storage::Error),
    #[error("Checkpoint writer thread panicked")]
    CheckpointWriterThreadPanicked,
    #[error("Checkpointed log reader error: {0}")]
    CheckpointedLogReader(#[from] CheckpointedLogReaderError),
    #[error("Cannot deserialize checkpoint: {0}")]
    CorruptedCheckpoint(#[source] bincode::error::DecodeError),
    #[error("Source {0} cannot restart. You have to clean data from previous runs by running `dozer clean`")]
    SourceCannotRestart(NodeHandle),
    #[error("Failed to create checkpoint: {0}")]
    FailedToCreateCheckpoint(BoxedError),
    #[error("Failed to serialize record writer: {0}")]
    SerializeRecordWriter(#[source] SerializationError),
}

impl<T> From<crossbeam::channel::SendError<T>> for ExecutionError {
    fn from(_: crossbeam::channel::SendError<T>) -> Self {
        ExecutionError::CannotSendToChannel
    }
}

impl<T> From<daggy::WouldCycle<T>> for ExecutionError {
    fn from(_: daggy::WouldCycle<T>) -> Self {
        ExecutionError::WouldCycle
    }
}
