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

    #[error("fetch of replication slot info failed. Error: {0}")]
    SyncWithSnapshotError(String),

    #[error("Replication stream error. Error: {0}")]
    ReplicationStreamError(String),

    #[error("Received unexpected message in replication stream")]
    UnexpectedReplicationMessageError,

    #[error("Replication stream error")]
    ReplicationStreamEndError,

    #[error(transparent)]
    PostgresSchemaError(#[from] PostgresSchemaError),
}

#[derive(Error, Debug)]
pub enum PostgresSchemaError {
    #[error("Schema's '{0}' replication identity settings is not correct. It is either not set or NOTHING")]
    SchemaReplicationIdentityError(String),

    #[error("Column type {0} not supported")]
    ColumnTypeNotSupported(String),

    #[error("Invalid column type")]
    InvalidColumnType,

    #[error("Value conversion error: {0}")]
    ValueConversionError(String),
}
