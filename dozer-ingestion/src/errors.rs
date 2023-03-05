#![allow(clippy::enum_variant_names)]

use dozer_types::errors::internal::BoxedError;
use dozer_types::errors::types::{SerializationError, TypeError};
use dozer_types::ingestion_types::IngestorError;
use dozer_types::thiserror::Error;
use dozer_types::{bincode, serde_json};
use dozer_types::{rust_decimal, thiserror};

use base64::DecodeError;

use datafusion::error::DataFusionError;
#[cfg(feature = "snowflake")]
use std::num::TryFromIntError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use deltalake::DeltaTableError;

use dozer_types::log::error;
#[cfg(feature = "snowflake")]
use odbc::DiagnosticRecord;
use schema_registry_converter::error::SRCError;
use tokio_postgres::Error;

#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("Missing `config` for connector {0}")]
    MissingConfiguration(String),

    #[error("Failed to map configuration")]
    WrongConnectionConfiguration,

    #[error("Unsupported grpc adapter: {0} {1}")]
    UnsupportedGrpcAdapter(String, String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Columns are expected in table_info")]
    ColumnsNotFound,

    #[error("Failed to initialize connector {0}")]
    InitializationError(String),

    #[error("This connector doesn't support this method: {0}")]
    UnsupportedConnectorMethod(String),

    #[error("Unexpected query message")]
    UnexpectedQueryMessageError,

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
    ObjectStoreConnectorError(#[from] ObjectStoreConnectorError),

    #[error(transparent)]
    TypeError(#[from] TypeError),

    #[error(transparent)]
    InternalError(#[from] BoxedError),

    #[error("Failed to send message on channel")]
    IngestorError(#[source] IngestorError),

    #[error("Error in Eth Connection: {0}")]
    EthError(#[source] web3::Error),

    #[error("Failed fetching after {0} recursions")]
    EthTooManyRecurisions(usize),

    #[error("Received empty message in connector")]
    EmptyMessage,

    #[error("Delta table error: {0}")]
    DeltaTableError(#[from] DeltaTableError),

    #[error("Datafusion error: {0}")]
    DataFusionError(#[from] DataFusionError),
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
#[derive(Error, Debug)]
pub enum ConfigurationError {
    #[error("Missing `config` for connector {0}")]
    MissingConfiguration(String),

    #[error("Failed to map configuration")]
    WrongConnectionConfiguration,
}

#[derive(Error, Debug)]
pub enum PostgresConnectorError {
    #[error("Query failed in connector: {0}")]
    InvalidQueryError(#[source] tokio_postgres::Error),

    #[error("Failed to connect to postgres with the specified configuration. {0}")]
    ConnectionFailure(#[source] tokio_postgres::Error),

    #[error("Replication is not available for user")]
    ReplicationIsNotAvailableForUserError,

    #[error("WAL level should be 'logical'")]
    WALLevelIsNotCorrect(),

    #[error("Cannot find table: {:?}", .0.join(", "))]
    TableError(Vec<String>),

    #[error("Cannot find column {0} in {1}")]
    ColumnNotFound(String, String),

    #[error("Cannot find columns {0}")]
    ColumnsNotFound(String),

    #[error("Failed to create a replication slot : {0}")]
    CreateSlotError(String, #[source] Error),

    #[error("Failed to create publication")]
    CreatePublicationError,

    #[error("Failed to drop publication")]
    DropPublicationError,

    #[error("Failed to begin txn for replication")]
    BeginReplication,

    #[error("Failed to begin txn for replication")]
    CommitReplication,

    #[error("fetch of replication slot info failed")]
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
    ReplicationStreamError(String),

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
}

#[derive(Error, Debug, Eq, PartialEq)]
pub enum PostgresSchemaError {
    #[error("Schema's '{0}' doesn't have primary key")]
    PrimaryKeyIsMissingInSchema(String),

    #[error("Table: '{0}' replication identity settings are not correct. It is either not set or NOTHING. Missing a primary key ?")]
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

    #[error("Invalid date")]
    InvalidDateError,

    #[error("Invalid time")]
    InvalidTimeError,

    #[error("Schema conversion Error: {0}")]
    SchemaConversionError(#[source] TryFromIntError),

    #[error("Decimal convert error")]
    DecimalConvertError(#[source] rust_decimal::Error),
}

#[derive(Error, Debug)]
pub enum SnowflakeStreamError {
    #[error("Time travel not available for table")]
    TimeTravelNotAvailableError,

    #[error("Unsupported \"{0}\" action in stream")]
    UnsupportedActionInStream(String),

    #[error("Cannot determine action")]
    CannotDetermineAction,

    #[error("Stream not found")]
    StreamNotFound,
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

    #[error("Schema registry fetch failed")]
    SchemaRegistryFetchError(#[source] SRCError),

    #[error("Topic not defined")]
    TopicNotDefined,
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

    #[error("Invalid date")]
    InvalidDateError,

    // #[error("Invalid time")]
    // InvalidTimeError,
    #[error("Invalid timestamp")]
    InvalidTimestampError,
}

#[derive(Error, Debug)]
pub enum ObjectStoreConnectorError {
    #[error(transparent)]
    DataFusionSchemaError(#[from] ObjectStoreSchemaError),

    #[error(transparent)]
    DataFusionStorageObjectError(#[from] ObjectStoreObjectError),

    #[error("Runtime creation error")]
    RuntimeCreationError,

    #[error("Internal data fusion error")]
    InternalDataFusionError(#[source] DataFusionError),

    #[error(transparent)]
    TableReaderError(#[from] ObjectStoreTableReaderError),

    #[error(transparent)]
    IngestorError(#[from] IngestorError),
}

#[derive(Error, Debug, PartialEq)]
pub enum ObjectStoreSchemaError {
    #[error("Unsupported type of \"{0}\" field")]
    FieldTypeNotSupported(String),

    #[error("Date time conversion failed")]
    DateTimeConversionError,

    #[error("Date conversion failed")]
    DateConversionError,

    #[error("Time conversion failed")]
    TimeConversionError,

    #[error("Duration conversion failed")]
    DurationConversionError,
}

#[derive(Error, Debug)]
pub enum ObjectStoreObjectError {
    #[error("Missing storage details")]
    MissingStorageDetails,

    #[error("Table definition not found")]
    TableDefinitionNotFound,

    #[error("Listing path parsing error: {0}")]
    ListingPathParsingError(#[source] DataFusionError),

    #[error("File format unsupported: {0}")]
    FileFormatUnsupportedError(String),
}

#[derive(Error, Debug)]
pub enum ObjectStoreTableReaderError {
    #[error("Table read failed")]
    TableReadFailed(DataFusionError),

    #[error("Columns select failed")]
    ColumnsSelectFailed(DataFusionError),

    #[error("Stream execution failed")]
    StreamExecutionError(DataFusionError),
}
