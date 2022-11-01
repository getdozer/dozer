#![allow(clippy::enum_variant_names)]
use crate::errors::internal::BoxedError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Columns are expected in table_info")]
    ColumnsNotFound,

    #[error("Failed to initialize connector")]
    InitializationError,

    #[error("Query failed")]
    InvalidQueryError,

    #[error("Schema Identifier is not present")]
    SchemaIdentifierNotFound,

    #[error(transparent)]
    PostgresConnectorError(#[from] PostgresConnectorError),

    #[error(transparent)]
    InternalError(#[from] BoxedError),
}

#[derive(Error, Debug)]
pub enum PostgresConnectorError {
    #[error("Failed to connect to database: {0}")]
    ConnectToDatabaseError(String),

    #[error("Replication is not available for user")]
    ReplicationIsNotAvailableForUserError(),

    #[error("WAL level should be 'logical'")]
    WALLevelIsNotCorrect(),

    #[error("Cannot find table: {:?}", .0.join(", "))]
    TableError(Vec<String>),

    #[error("Failed to create a replication slot : {0}")]
    CreateSlotError(String),

    #[error("Failed to create publication")]
    CreatePublicationError,

    #[error("Failed to drop publication")]
    DropPublicationError,

    #[error("Failed to begin txn for replication")]
    BeginReplication,

    #[error("Failed to begin txn for replication")]
    CommitReplication,

    #[error("fetch of replication slot info failed")]
    FetchReplicationSlot,

    #[error("No slots available or all available slots are used")]
    NoAvailableSlotsError,

    #[error("Slot {0} not found")]
    SlotNotExistError(String),

    #[error("Slot {0} is already used by another process")]
    SlotIsInUseError(String),

    #[error("Start lsn is before first available lsn - {0} < {1}")]
    StartLsnIsBeforeLastFlushedLsnError(String, String),
}
