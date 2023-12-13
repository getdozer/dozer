use datafusion::{datasource::listing::ListingTableUrl, error::DataFusionError};
use dozer_ingestion_connector::dozer_types::{
    arrow_types::errors::FromArrowError,
    thiserror::{self, Error},
};

mod adapters;
mod connection;
pub mod connector;
mod helper;
mod schema_helper;
pub mod schema_mapper;
mod table;
mod table_reader;
pub(crate) mod table_watcher;
#[cfg(test)]
mod tests;

#[derive(Error, Debug)]
pub enum ObjectStoreConnectorError {
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    DataFusionSchemaError(#[from] ObjectStoreSchemaError),

    #[error(transparent)]
    DataFusionStorageObjectError(#[from] ObjectStoreObjectError),

    #[error("Internal data fusion error")]
    InternalDataFusionError(#[source] DataFusionError),

    #[error(transparent)]
    TableReaderError(#[from] ObjectStoreTableReaderError),

    #[error(transparent)]
    FromArrowError(#[from] FromArrowError),

    #[error("Failed to send message on data read channel")]
    SendError,

    #[error("Failed to receive message on data read channel")]
    RecvError,
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

    #[error("Listing path {0} parsing error: {1}")]
    ListingPathParsingError(String, #[source] DataFusionError),

    #[error("File format unsupported: {0}")]
    FileFormatUnsupportedError(String),

    #[error("Listing path {0} error: {1}")]
    ListingPathError(String, #[source] DataFusionError),
}

#[derive(Error, Debug)]
pub enum ObjectStoreTableReaderError {
    #[error("Table read failed: {0}")]
    TableReadFailed(DataFusionError),

    #[error("Columns select failed: {0}")]
    ColumnsSelectFailed(DataFusionError),

    #[error("Stream execution failed: {0}")]
    StreamExecutionError(DataFusionError),

    #[error("File {0} has a conflicting schema")]
    ConflictingSchema(ListingTableUrl),
}
