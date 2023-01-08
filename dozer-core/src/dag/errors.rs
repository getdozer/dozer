#![allow(clippy::enum_variant_names)]
use crate::dag::appsource::AppSourceId;
use crate::dag::node::{NodeHandle, PortHandle};
use crate::storage::errors::StorageError;
use dozer_types::errors::internal::BoxedError;
use dozer_types::errors::types::TypeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("Invalid port handle: {0}")]
    InvalidPortHandle(PortHandle),
    #[error("Invalid node handle: {0}")]
    InvalidNodeHandle(NodeHandle),
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    #[error("Schema not initialized")]
    SchemaNotInitialized,
    #[error("The node {0} does not have any input")]
    MissingNodeInput(NodeHandle),
    #[error("The node {0} does not have any output")]
    MissingNodeOutput(NodeHandle),
    #[error("The node type is invalid")]
    InvalidNodeType,
    #[error("The database is invalid")]
    InvalidDatabase,
    #[error("Field not found at position {0}")]
    FieldNotFound(String),
    #[error("Port not found in source for schema_id: {0}.")]
    PortNotFound(String),
    #[error("Record not found")]
    RecordNotFound(),
    #[error("Invalid checkpoint state for node: {0}")]
    InvalidCheckpointState(NodeHandle),
    #[error("Already exists: {0}")]
    MetadataAlreadyExists(NodeHandle),
    #[error("Incompatible schemas")]
    IncompatibleSchemas(),
    #[error("Channel disconnected")]
    ChannelDisconnected,
    #[error("Cannot spawn worker thread: {0}")]
    CannotSpawnWorkerThread(#[from] std::io::Error),
    #[error("Internal thread panicked")]
    InternalThreadPanic,
    #[error("Invalid source identifier {0}")]
    InvalidSourceIdentifier(AppSourceId),
    #[error("Ambiguous source identifier {0}")]
    AmbiguousSourceIdentifier(AppSourceId),
    #[error("Inconsistent checkpointing data")]
    InconsistentCheckpointMetadata,
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

    // Error forwarders
    #[error(transparent)]
    InternalTypeError(#[from] TypeError),
    #[error(transparent)]
    InternalDatabaseError(#[from] StorageError),
    #[error(transparent)]
    InternalError(#[from] BoxedError),
    #[error("{0}. Has dozer been initialized (`dozer init`)?")]
    SinkError(#[source] SinkError),

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
}

#[derive(Error, Debug)]
pub enum SinkError {
    #[error("Failed to initialize schema in Sink: {0}")]
    SchemaUpdateFailed(#[source] BoxedError),

    #[error("Failed to begin cache transaction: {0}")]
    CacheBeginTransactionFailed(#[source] BoxedError),

    #[error("Failed to insert record in Sink: {0}")]
    CacheInsertFailed(#[source] BoxedError),

    #[error("Failed to delete record in Sink: {0}")]
    CacheDeleteFailed(#[source] BoxedError),

    #[error("Failed to update record in Sink: {0}")]
    CacheUpdateFailed(#[source] BoxedError),

    #[error("Failed to commit cache transaction: {0}")]
    CacheCommitTransactionFailed(#[source] BoxedError),
}
