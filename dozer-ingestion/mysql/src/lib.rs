use dozer_ingestion_connector::dozer_types::{
    errors::types::DeserializationError,
    thiserror::{self, Error},
};
use geozero::error::GeozeroError;

mod binlog;
mod connection;
pub mod connector;
mod conversion;
pub(crate) mod helpers;
mod schema;
#[cfg(test)]
mod tests;

#[derive(Error, Debug)]
pub enum MySQLConnectorError {
    #[error("Invalid connection URL: {0:?}")]
    InvalidConnectionURLError(#[source] mysql_async::UrlError),

    #[error("Failed to connect to mysql with the specified url {0}. {1}")]
    ConnectionFailure(String, #[source] mysql_async::Error),

    #[error("Unsupported field type: {0}")]
    UnsupportedFieldType(String),

    #[error("Invalid field value. {0}")]
    InvalidFieldValue(#[from] mysql_common::FromValueError),

    #[error("Invalid json value. {0}")]
    JsonDeserializationError(#[from] DeserializationError),

    #[error("Invalid geometric value. {0}")]
    InvalidGeometricValue(#[from] GeozeroError),

    #[error("Failed to open binlog. {0}")]
    BinlogOpenError(#[source] mysql_async::Error),

    #[error("Failed to read binlog. {0}")]
    BinlogReadError(#[source] mysql_async::Error),

    #[error("Binlog error: {0}")]
    BinlogError(String),

    #[error("Query failed. {0}")]
    QueryExecutionError(#[source] mysql_async::Error),

    #[error("Failed to fetch query result. {0}")]
    QueryResultError(#[source] mysql_async::Error),
}
