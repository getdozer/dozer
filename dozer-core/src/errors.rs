use std::path::PathBuf;

use crate::node::PortHandle;
use dozer_storage::errors::StorageError;
use dozer_types::errors::internal::BoxedError;
use dozer_types::node::NodeHandle;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;

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
    #[error("Source error: {0}")]
    Source(#[source] BoxedError),
    #[error("File system error {0:?}: {1}")]
    FileSystemError(PathBuf, #[source] std::io::Error),
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Object storage error: {0}")]
    ObjectStorage(#[from] dozer_log::storage::Error),
    #[error("Checkpoint writer thread panicked")]
    CheckpointWriterThreadPanicked,
    #[error("Unrecognized checkpoint: {0}")]
    UnrecognizedCheckpoint(String),
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
