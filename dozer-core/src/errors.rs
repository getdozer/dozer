use std::path::PathBuf;

use crate::appsource::AppSourceId;
use crate::node::PortHandle;
use dozer_types::errors::internal::BoxedError;
use dozer_types::errors::types::TypeError;
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
    #[error("Invalid type: {0}")]
    InvalidType(String),
    #[error("Schema not initialized")]
    SchemaNotInitialized,
    #[error("Record not found")]
    RecordNotFound(),
    #[error("Cannot send to channel")]
    CannotSendToChannel,
    #[error("Cannot receive from channel")]
    CannotReceiveFromChannel,
    #[error("Cannot spawn worker thread: {0}")]
    CannotSpawnWorkerThread(#[source] std::io::Error),
    #[error("Invalid source identifier {0}")]
    InvalidSourceIdentifier(AppSourceId),
    #[error("Ambiguous source identifier {0}")]
    AmbiguousSourceIdentifier(AppSourceId),
    #[error("Port not found for source: {0}")]
    PortNotFoundInSource(PortHandle),
    #[error("Invalid AppSource connection {0}. Already exists.")]
    AppSourceConnectionAlreadyExists(String),

    // Error forwarders
    #[error("File system error {0:?}: {1}")]
    FileSystemError(PathBuf, #[source] std::io::Error),
    #[error("Internal type error: {0}")]
    InternalTypeError(#[from] TypeError),
    #[error("Internal error: {0}")]
    InternalError(#[from] BoxedError),

    #[error("Failed to initialize source: {0}")]
    ConnectorError(#[source] BoxedError),
    // to remove
    #[error("{0}")]
    InternalStringError(String),

    #[error("Source error: {0}")]
    SourceError(SourceError),

    #[error("Failed to execute product processor: {0}")]
    ProductProcessorError(#[source] BoxedError),

    #[error("Failed to create Window processor: {0}")]
    WindowProcessorFactoryError(#[source] BoxedError),

    #[error("Failed to execute the Window processor: {0}")]
    WindowProcessorError(#[source] BoxedError),

    #[error("Failed to execute the Table processor: {0}")]
    TableProcessorError(#[source] BoxedError),

    #[error("JOIN processor received a Record from a wrong input: {0}")]
    InvalidPort(u16),

    #[error("Error variant for testing: {0}")]
    TestError(String),
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

#[derive(Error, Debug)]
pub enum SourceError {
    #[error("Failed to find table in Source: {0}")]
    PortError(u32),
}
