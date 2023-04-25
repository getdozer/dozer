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
    #[error("Invalid node handle: {0}")]
    InvalidNodeHandle(NodeHandle),
    #[error("Missing input for node {node} on port {port}")]
    MissingInput { node: NodeHandle, port: PortHandle },
    #[error("Duplicate input for node {node} on port {port}")]
    DuplicateInput { node: NodeHandle, port: PortHandle },
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    #[error("Invalid type: {0}")]
    InvalidType(String),
    #[error("Schema not initialized")]
    SchemaNotInitialized,
    #[error("The database is invalid")]
    InvalidDatabase,
    #[error("Field not found at position {0}")]
    FieldNotFound(String),
    #[error("Record not found")]
    RecordNotFound(),
    #[error("Cannot send to channel")]
    CannotSendToChannel,
    #[error("Cannot receive from channel")]
    CannotReceiveFromChannel,
    #[error("Cannot spawn worker thread: {0}")]
    CannotSpawnWorkerThread(#[from] std::io::Error),
    #[error("Internal thread panicked")]
    InternalThreadPanic,
    #[error("Invalid source identifier {0}")]
    InvalidSourceIdentifier(AppSourceId),
    #[error("Ambiguous source identifier {0}")]
    AmbiguousSourceIdentifier(AppSourceId),
    #[error("Port not found for source: {0}")]
    PortNotFoundInSource(PortHandle),
    #[error("Failed to get output schema: {0}")]
    FailedToGetOutputSchema(String),
    #[error("Update operation not supported: {0}")]
    UnsupportedUpdateOperation(String),
    #[error("Delete operation not supported: {0}")]
    UnsupportedDeleteOperation(String),
    #[error("Invalid AppSource connection {0}. Already exists.")]
    AppSourceConnectionAlreadyExists(String),
    #[error("Failed to get primary key for `{0}`")]
    FailedToGetPrimaryKey(String),

    // Error forwarders
    #[error("File system error {0:?}: {1}")]
    FileSystemError(PathBuf, #[source] std::io::Error),
    #[error("Internal type error: {0}")]
    InternalTypeError(#[from] TypeError),
    #[error("Internal error: {0}")]
    InternalError(#[from] BoxedError),
    #[error("Sink error: {0}")]
    SinkError(#[from] SinkError),

    #[error("Failed to initialize source: {0}")]
    ConnectorError(#[source] BoxedError),
    // to remove
    #[error("{0}")]
    InternalStringError(String),

    #[error("Channel returned empty message in sink. Might be an issue with the sender: {0}, {1}")]
    SinkReceiverError(usize, #[source] BoxedError),

    #[error(
        "Channel returned empty message in processor. Might be an issue with the sender: {0}, {1}"
    )]
    ProcessorReceiverError(usize, #[source] BoxedError),

    #[error("Source error: {0}")]
    SourceError(SourceError),

    #[error("Failed to execute product processor: {0}")]
    ProductProcessorError(#[source] BoxedError),

    #[error("Failed to create Window processor: {0}")]
    WindowProcessorFactoryError(#[source] BoxedError),

    #[error("Failed to execute the Window processor: {0}")]
    WindowProcessorError(#[source] BoxedError),

    #[error("JOIN processor received a Record from a wrong input: {0}")]
    InvalidPort(u16),
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
pub enum SinkError {
    #[error("Failed to open Cache: {0:?}, Error: {1:?}.")]
    CacheOpenFailed(String, #[source] BoxedError),

    #[error("Failed to create Cache: {0:?}, Error: {1:?}.")]
    CacheCreateFailed(String, #[source] BoxedError),

    #[error("Failed to create alias {alias:?} for Cache: {real_name:?}, Error: {source:?}.")]
    CacheCreateAliasFailed {
        alias: String,
        real_name: String,
        #[source]
        source: BoxedError,
    },

    #[error("Failed to insert record in Cache: {0:?}, Error: {1:?}. Usually this happens if primary key is wrongly specified.")]
    CacheInsertFailed(String, #[source] BoxedError),

    #[error("Failed to delete record in Cache: {0:?}, Error: {1:?}. Usually this happens if primary key is wrongly specified.")]
    CacheDeleteFailed(String, #[source] BoxedError),

    #[error("Failed to update record in Cache: {0:?}, Error: {1:?}. Usually this happens if primary key is wrongly specified.")]
    CacheUpdateFailed(String, #[source] BoxedError),

    #[error("Failed to commit cache transaction: {0:?}, Error: {1:?}")]
    CacheCommitTransactionFailed(String, #[source] BoxedError),

    #[error("Cache {0} has reached its maximum size. Try to increase `cache_max_map_size` in the config.")]
    CacheFull(String),

    #[error("Failed to count the records during init in Cache: {0:?}, Error: {1:?}")]
    CacheCountFailed(String, #[source] BoxedError),
}

#[derive(Error, Debug)]
pub enum SourceError {
    #[error("Failed to find table in Source: {0}")]
    PortError(u32),
}
