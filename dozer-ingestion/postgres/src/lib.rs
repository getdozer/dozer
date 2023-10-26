use std::string::FromUtf8Error;

use dozer_ingestion_connector::dozer_types::{
    chrono,
    errors::types::{DeserializationError, TypeError},
    thiserror::{self, Error},
};
use tokio_postgres::config::SslMode;

pub mod connection;
pub mod connector;
pub mod helper;
pub mod iterator;
mod replication_slot_helper;
pub mod replicator;
mod schema;
pub mod snapshotter;
#[cfg(test)]
pub mod test_utils;
#[cfg(test)]
pub mod tests;
pub mod xlog_mapper;

pub use tokio_postgres;

#[derive(Error, Debug)]
pub enum PostgresConnectorError {
    #[error("Failed to map configuration: {0}")]
    WrongConnectionConfiguration(DeserializationError),

    #[error("Invalid SslMode: {0:?}")]
    InvalidSslError(SslMode),

    #[error("Query failed in connector: {0}")]
    InvalidQueryError(#[source] tokio_postgres::Error),

    #[error("Failed to connect to postgres with the specified configuration. {0}")]
    ConnectionFailure(#[source] tokio_postgres::Error),

    #[error("Replication is not available for user")]
    ReplicationIsNotAvailableForUserError,

    #[error("WAL level should be 'logical'")]
    WALLevelIsNotCorrect(),

    #[error("Cannot find tables {0:?}")]
    TablesNotFound(Vec<(String, String)>),

    #[error("Cannot find column {0} in {1}")]
    ColumnNotFound(String, String),

    #[error("Cannot find columns {0}")]
    ColumnsNotFound(String),

    #[error("Failed to create a replication slot \"{0}\". Error: {1}")]
    CreateSlotError(String, #[source] tokio_postgres::Error),

    #[error("Failed to create publication: {0}")]
    CreatePublicationError(#[source] tokio_postgres::Error),

    #[error("Failed to drop publication: {0}")]
    DropPublicationError(#[source] tokio_postgres::Error),

    #[error("Failed to begin txn for replication")]
    BeginReplication,

    #[error("Failed to begin txn for replication")]
    CommitReplication,

    #[error("Fetch of replication slot info failed. Error: {0}")]
    FetchReplicationSlotError(#[source] tokio_postgres::Error),

    #[error("No slots available or all available slots are used")]
    NoAvailableSlotsError,

    #[error("Slot {0} not found")]
    SlotNotExistError(String),

    #[error("Slot {0} is already used by another process")]
    SlotIsInUseError(String),

    #[error("Table {0} changes is not replicated to slot")]
    MissingTableInReplicationSlot(String),

    #[error("Start lsn is before first available lsn - {0} < {1}")]
    StartLsnIsBeforeLastFlushedLsnError(String, String),

    #[error("fetch of replication slot info failed. Error: {0}")]
    SyncWithSnapshotError(String),

    #[error("Replication stream error. Error: {0}")]
    ReplicationStreamError(tokio_postgres::Error),

    #[error("Received unexpected message in replication stream")]
    UnexpectedReplicationMessageError,

    #[error("Replication stream error")]
    ReplicationStreamEndError,

    #[error(transparent)]
    PostgresSchemaError(#[from] PostgresSchemaError),

    #[error("LSN not stored for replication slot")]
    LSNNotStoredError,

    #[error("LSN parse error. Given lsn: {0}")]
    LsnParseError(String),

    #[error("LSN not returned from replication slot creation query")]
    LsnNotReturnedFromReplicationSlot,

    #[error("Table name \"{0}\" not valid")]
    TableNameNotValid(String),

    #[error("Column name \"{0}\" not valid")]
    ColumnNameNotValid(String),

    #[error("Relation not found in replication: {0}")]
    RelationNotFound(#[source] std::io::Error),

    #[error("Failed to send message on snapshot read channel")]
    SnapshotReadError,

    #[error("Failed to load native certs: {0}")]
    LoadNativeCerts(#[source] std::io::Error),

    #[error("Non utf8 column name in table {table_index} column {column_index}")]
    NonUtf8ColumnName {
        table_index: usize,
        column_index: usize,
    },

    #[error("Column type changed in table {table_index} column {column_name} from {old_type} to {new_type}")]
    ColumnTypeChanged {
        table_index: usize,
        column_name: String,
        old_type: postgres_types::Type,
        new_type: postgres_types::Type,
    },

    #[error("Unexpected query message")]
    UnexpectedQueryMessageError,
}

#[derive(Error, Debug)]
pub enum PostgresSchemaError {
    #[error("Schema's '{0}' doesn't have primary key")]
    PrimaryKeyIsMissingInSchema(String),

    #[error("Table: '{0}' replication identity settings are not correct. It is either not set or NOTHING. Missing a primary key ?")]
    SchemaReplicationIdentityError(String),

    #[error("Column type {0} not supported")]
    ColumnTypeNotSupported(String),

    #[error("Custom type {0:?} is not supported yet. Join our Discord at https://discord.com/invite/3eWXBgJaEQ - we're here to help with your use case!")]
    CustomTypeNotSupported(String),

    #[error("ColumnTypeNotFound")]
    ColumnTypeNotFound,

    #[error("Invalid column type of column {0}")]
    InvalidColumnType(String),

    #[error("Value conversion error: {0}")]
    ValueConversionError(String),

    #[error("String parse failed")]
    StringParseError(#[source] FromUtf8Error),

    #[error("JSONB parse failed: {0}")]
    JSONBParseError(String),

    #[error("Point parse failed")]
    PointParseError,

    #[error("Unsupported replication type - '{0}'")]
    UnsupportedReplicationType(String),

    #[error(
        "Table type '{0}' of '{1}' table is not supported. Only 'BASE TABLE' type is supported"
    )]
    UnsupportedTableType(String, String),

    #[error("Table type cannot be determined")]
    TableTypeNotFound,

    #[error("Column not found")]
    ColumnNotFound,

    #[error("Type error: {0}")]
    TypeError(#[from] TypeError),

    #[error("Failed to read string from utf8. Error: {0}")]
    StringReadError(#[from] FromUtf8Error),

    #[error("Failed to read date. Error: {0}")]
    DateReadError(#[from] chrono::ParseError),
}
