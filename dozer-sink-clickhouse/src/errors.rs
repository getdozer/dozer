use dozer_types::thiserror::{self, Error};

#[derive(Error, Debug)]
pub enum ClickhouseSinkError {
    #[error("Only MergeTree engine is supported for delete operation")]
    UnsupportedOperation,

    #[error("Column {0} not found in sink table")]
    ColumnNotFound(String),

    #[error("Column {0} has type {1} in dozer schema but type {2} in sink table")]
    ColumnTypeMismatch(String, String, String),

    #[error("Clickhouse error: {0:?}")]
    ClickhouseError(#[from] clickhouse_rs::errors::Error),

    #[error("Primary key not found")]
    PrimaryKeyNotFound,

    #[error("Sink table does not exist and create_table_options is not set")]
    SinkTableDoesNotExist,

    #[error("Expected primary key {0:?} but got {1:?}")]
    PrimaryKeyMismatch(Vec<String>, Vec<String>),

    #[error("QueryError: {0:?}")]
    QueryError(#[from] QueryError),
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Clickhouse error: {0:?}")]
    DataFetchError(#[from] clickhouse_rs::errors::Error),

    #[error("Unexpected field type for {0:?}, expected {0}")]
    TypeMismatch(String, String),

    #[error("{0:?}")]
    CustomError(String),
}
