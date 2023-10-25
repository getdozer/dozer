use std::num::TryFromIntError;

use dozer_ingestion_connector::dozer_types::{
    rust_decimal,
    thiserror::{self, Error},
};
use odbc::DiagnosticRecord;

pub mod connection;
pub mod connector;
mod schema_helper;
pub mod stream_consumer;
pub mod test_utils;

#[cfg(test)]
mod tests;

#[derive(Error, Debug)]
pub enum SnowflakeError {
    #[error("Snowflake query error")]
    QueryError(#[source] Box<DiagnosticRecord>),

    #[error("Snowflake connection error")]
    ConnectionError(#[source] Box<DiagnosticRecord>),

    #[error(transparent)]
    SnowflakeSchemaError(#[from] SnowflakeSchemaError),

    #[error(transparent)]
    SnowflakeStreamError(#[from] SnowflakeStreamError),

    #[error("A network error occurred, but this query is not resumable. query: {0}")]
    NonResumableQuery(String),
}

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
