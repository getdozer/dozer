#![allow(clippy::enum_variant_names)]

use dozer_types::errors::internal::BoxedError;
use dozer_types::errors::types::{SerializationError, TypeError};
use dozer_types::ingestion_types::IngestorError;
use dozer_types::thiserror::Error;
use dozer_types::{bincode, serde_json};
use dozer_types::{rust_decimal, thiserror};

use base64::DecodeError;
#[cfg(feature = "snowflake")]
use std::num::TryFromIntError;
use std::str::Utf8Error;

#[cfg(feature = "snowflake")]
use odbc::DiagnosticRecord;

#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Columns are expected in table_info")]
    ColumnsNotFound,

    #[error("Relation not found in replication")]
    RelationNotFound(#[source] std::io::Error),

    #[error("Failed to initialize connector")]
    InitializationError,

    #[error("Failed to map configuration")]
    WrongConnectionConfiguration,

    #[error("This connector doesn't support this method: {0}")]
    UnsupportedConnectorMethod(String),

    #[error("Query failed")]
    InvalidQueryError,

    #[error("Schema Identifier is not present")]
    SchemaIdentifierNotFound,

    #[error(transparent)]
    PostgresConnectorError(#[from] PostgresConnectorError),

    #[cfg(feature = "snowflake")]
    #[error(transparent)]
    SnowflakeError(#[from] SnowflakeError),

    #[error(transparent)]
    DebeziumError(#[from] DebeziumError),

    #[error(transparent)]
    TypeError(#[from] TypeError),

    #[error(transparent)]
    InternalError(#[from] BoxedError),

    #[error("Failed to send message on channel")]
    IngestorError(#[source] IngestorError),

    #[error("Error in Eth Connection")]
    EthError(#[source] web3::Error),

    #[error("Received empty message in connector")]
    EmptyMessage,
}
impl ConnectorError {
    pub fn map_serialization_error(e: serde_json::Error) -> ConnectorError {
        ConnectorError::TypeError(TypeError::SerializationError(SerializationError::Json(e)))
    }

    pub fn map_bincode_serialization_error(e: bincode::Error) -> ConnectorError {
        ConnectorError::TypeError(TypeError::SerializationError(SerializationError::Bincode(
            e,
        )))
    }
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum PostgresConnectorError {
    #[error("Failed to connect to database: {0}")]
    ConnectToDatabaseError(String),

    #[error("Replication is not available for user")]
    ReplicationIsNotAvailableForUserError,

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

    #[error("LSN not stored for replication slot")]
    LSNNotStoredError,
}

#[derive(Error, Debug, Eq, PartialEq)]
pub enum PostgresSchemaError {
    #[error("Schema's '{0}' replication identity settings is not correct. It is either not set or NOTHING")]
    SchemaReplicationIdentityError(String),

    #[error("Column type {0} not supported")]
    ColumnTypeNotSupported(String),

    #[error("CustomTypeNotSupported")]
    CustomTypeNotSupported,

    #[error("ColumnTypeNotFound")]
    ColumnTypeNotFound,

    #[error("Invalid column type")]
    InvalidColumnType,

    #[error("Value conversion error: {0}")]
    ValueConversionError(String),
}

#[cfg(feature = "snowflake")]
#[derive(Error, Debug)]
pub enum SnowflakeError {
    #[error("Snowflake query error")]
    QueryError(#[source] Box<DiagnosticRecord>),

    #[error("Snowflake connection error")]
    ConnectionError(#[from] Box<DiagnosticRecord>),

    #[cfg(feature = "snowflake")]
    #[error(transparent)]
    SnowflakeSchemaError(#[from] SnowflakeSchemaError),

    #[error(transparent)]
    SnowflakeStreamError(#[from] SnowflakeStreamError),
}

#[cfg(feature = "snowflake")]
#[derive(Error, Debug)]
pub enum SnowflakeSchemaError {
    #[error("Column type {0} not supported")]
    ColumnTypeNotSupported(String),

    #[error("Value conversion Error")]
    ValueConversionError(#[source] Box<DiagnosticRecord>),

    #[error("Schema conversion Error")]
    SchemaConversionError(#[source] TryFromIntError),
}

#[derive(Error, Debug)]
pub enum SnowflakeStreamError {
    #[error("Unsupported \"{0}\" action in stream")]
    UnsupportedActionInStream(String),

    #[error("Cannot determine action")]
    CannotDetermineAction,
}

#[derive(Error, Debug)]
pub enum DebeziumError {
    #[error(transparent)]
    DebeziumSchemaError(#[from] DebeziumSchemaError),

    #[error("Connection error")]
    DebeziumConnectionError(#[source] kafka::Error),

    #[error("JSON decode error")]
    JsonDecodeError(#[source] serde_json::Error),

    #[error("Bytes convert error")]
    BytesConvertError(#[source] Utf8Error),

    #[error(transparent)]
    DebeziumStreamError(#[from] DebeziumStreamError),
}

#[derive(Error, Debug)]
pub enum DebeziumStreamError {
    #[error("Consume commit error")]
    ConsumeCommitError(#[source] kafka::Error),

    #[error("Message consume error")]
    MessageConsumeError(#[source] kafka::Error),

    #[error("Polling error")]
    PollingError(#[source] kafka::Error),
}

#[derive(Error, Debug, PartialEq)]
pub enum DebeziumSchemaError {
    #[error("Schema definition not found")]
    SchemaDefinitionNotFound,

    #[error("Unsupported \"{0}\" type")]
    TypeNotSupported(String),

    #[error("Field \"{0}\" not found")]
    FieldNotFound(String),

    #[error("Binary decode error")]
    BinaryDecodeError(#[source] DecodeError),

    #[error("Scale not found")]
    ScaleNotFound,

    #[error("Scale is invalid")]
    ScaleIsInvalid,

    #[error("Decimal convert error")]
    DecimalConvertError(#[source] rust_decimal::Error),
}
