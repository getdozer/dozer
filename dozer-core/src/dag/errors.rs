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
    #[error("Port not found in source {0}")]
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

    // Error forwarders
    #[error(transparent)]
    InternalTypeError(#[from] TypeError),
    #[error(transparent)]
    InternalDatabaseError(#[from] StorageError),
    #[error(transparent)]
    InternalError(#[from] BoxedError),
    #[error(transparent)]
    SinkError(#[from] SinkError),

    #[error("Failed to initialize source")]
    ConnectorError(#[source] BoxedError),
    // to remove
    #[error("{0}")]
    InternalStringError(String),

    #[error("Channel returned empty message in sink. Might be an issue with the sender: {0}")]
    SinkReceiverError(usize, #[source] BoxedError),

    #[error("Channel returned empty message in processor. Might be an issue with the sender: {0}")]
    ProcessorReceiverError(usize, #[source] BoxedError),
}

#[derive(Error, Debug)]
pub enum SinkError {
    #[error("Failed to initialize schema in Sink")]
    SchemaUpdateFailed(#[source] BoxedError),

    #[error("Failed to notify schema in Sink")]
    SchemaNotificationFailed(#[source] BoxedError),

    #[error("Failed to begin cache transaction")]
    CacheBeginTransactionFailed(#[source] BoxedError),

    #[error("Failed to insert record in Sink")]
    CacheInsertFailed(#[source] BoxedError),

    #[error("Failed to delete record in Sink")]
    CacheDeleteFailed(#[source] BoxedError),

    #[error("Failed to update record in Sink")]
    CacheUpdateFailed(#[source] BoxedError),

    #[error("Failed to commit cache transaction")]
    CacheCommitTransactionFailed(#[source] BoxedError),
}
