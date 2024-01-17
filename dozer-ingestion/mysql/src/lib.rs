use dozer_ingestion_connector::dozer_types::{
    errors::types::DeserializationError,
    thiserror::{self, Error},
    types::FieldType,
};
use geozero::error::GeozeroError;

mod binlog;
mod connection;
pub mod connector;
mod conversion;
pub(crate) mod helpers;
mod schema;
mod state;
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

    #[error("Schema had a breaking change: {0}")]
    BreakingSchemaChange(#[from] BreakingSchemaChange),

    #[error("Failed to send snapshot completed ingestion message")]
    SnapshotIngestionMessageError,
}

#[derive(Error, Debug)]
pub enum BreakingSchemaChange {
    #[error("Database \"{0}\" was dropped")]
    DatabaseDropped(String),
    #[error("Table \"{0}\" was dropped")]
    TableDropped(String),
    #[error("Table \"{0}\" has been dropped or renamed")]
    TableDroppedOrRenamed(String),
    #[error("Multiple tables have been dropped or renamed: {}", .0.join(", "))]
    MultipleTablesDroppedOrRenamed(Vec<String>),
    #[error("Table \"{old_table_name}\" was renamed to \"{new_table_name}\"")]
    TableRenamed {
        old_table_name: String,
        new_table_name: String,
    },
    #[error("Column \"{column_name}\" from table \"{table_name}\" was dropped")]
    ColumnDropped {
        table_name: String,
        column_name: String,
    },
    #[error("Column \"{old_column_name}\" from table \"{table_name}\" was renamed to \"{new_column_name}\"")]
    ColumnRenamed {
        table_name: String,
        old_column_name: String,
        new_column_name: String,
    },
    #[error("Column \"{column_name}\" from table \"{table_name}\" has been dropped or renamed")]
    ColumnDroppedOrRenamed {
        table_name: String,
        column_name: String,
    },
    #[error("Multiple columns from table \"{table_name}\" have been dropped or renamed: {}", .columns.join(", "))]
    MultipleColumnsDroppedOrRenamed {
        table_name: String,
        columns: Vec<String>,
    },
    #[error("Column \"{column_name}\" from table \"{table_name}\" changed data type from \"{old_data_type}\" to \"{new_column_name}\"")]
    ColumnDataTypeChanged {
        table_name: String,
        column_name: String,
        old_data_type: FieldType,
        new_column_name: FieldType,
    },
}

#[derive(Error, Debug)]
pub enum MysqlStateError {
    #[error("Failed to read binlog position from state. Error: {0}")]
    TrySliceError(#[from] std::array::TryFromSliceError),
}
