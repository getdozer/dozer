use crate::appsource::AppSourceId;
use crate::dag_metadata::SchemaType;
use crate::node::PortHandle;
use dozer_storage::errors::StorageError;
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
    #[error("Port not found in source for schema_id: {0}.")]
    PortNotFound(String),
    #[error("Replication type not found")]
    ReplicationTypeNotFound,
    #[error("Record not found")]
    RecordNotFound(),
    #[error("Node {node} has incompatible {typ:?} schemas: {source}")]
    IncompatibleSchemas {
        node: NodeHandle,
        typ: SchemaType,
        source: IncompatibleSchemas,
    },
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
    #[error("Got mismatching primary key for `{endpoint_name}`. Expected: `{expected:?}`, got: `{actual:?}`")]
    MismatchPrimaryKey {
        endpoint_name: String,
        expected: Vec<String>,
        actual: Vec<String>,
    },

    // Error forwarders
    #[error(transparent)]
    InternalTypeError(#[from] TypeError),
    #[error(transparent)]
    InternalDatabaseError(#[from] StorageError),
    #[error("internal error: {0}")]
    InternalError(#[from] BoxedError),
    #[error(transparent)]
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

    #[error(transparent)]
    JoinError(JoinError),

    #[error(transparent)]
    SourceError(SourceError),

    #[error("Failed to execute product processor: {0}")]
    ProductProcessorError(#[source] BoxedError),
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
pub enum IncompatibleSchemas {
    #[error("Length mismatch: current {current}, existing {existing}")]
    LengthMismatch { current: usize, existing: usize },
    #[error("Not found on port {0}")]
    NotFound(PortHandle),
    #[error("Schema mismatch on port {0}")]
    SchemaMismatch(PortHandle),
}

#[derive(Error, Debug)]
pub enum SinkError {
    #[error("Failed to initialize schema in Cache: {0:?}, Error: {1:?}.")]
    SchemaUpdateFailed(String, #[source] BoxedError),

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

    #[error("Failed to get checkpoint in Cache: {0:?}, Error: {1:?}.")]
    CacheGetCheckpointFailed(String, #[source] BoxedError),

    #[error("Failed to begin transaction in Cache: {0:?}, Error: {1:?}.")]
    CacheBeginTransactionFailed(String, #[source] BoxedError),

    #[error("Failed to insert record in Cache: {0:?}, Error: {1:?}. Usually this happens if primary key is wrongly specified.")]
    CacheInsertFailed(String, #[source] BoxedError),

    #[error("Failed to delete record in Cache: {0:?}, Error: {1:?}. Usually this happens if primary key is wrongly specified.")]
    CacheDeleteFailed(String, #[source] BoxedError),

    #[error("Failed to update record in Cache: {0:?}, Error: {1:?}. Usually this happens if primary key is wrongly specified.")]
    CacheUpdateFailed(String, #[source] BoxedError),

    #[error("Failed to commit cache transaction: {0:?}, Error: {1:?}")]
    CacheCommitTransactionFailed(String, #[source] BoxedError),

    #[error("Failed to count thre records during init in Cache: {0:?}, Error: {1:?}")]
    CacheCountFailed(String, #[source] BoxedError),
}

#[derive(Error, Debug)]
pub enum JoinError {
    #[error("Failed to find table in Join during Insert: {0}")]
    InsertPortError(PortHandle),
    #[error("Failed to find table in Join during Delete: {0}")]
    DeletePortError(PortHandle),
    #[error("Failed to find table in Join during Update: {0}")]
    UpdatePortError(PortHandle),
    #[error("Join ports are not properly initialized")]
    PortNotConnected(PortHandle),
}

#[derive(Error, Debug)]
pub enum SourceError {
    #[error("Failed to find table in Source: {0}")]
    PortError(u32),
}
