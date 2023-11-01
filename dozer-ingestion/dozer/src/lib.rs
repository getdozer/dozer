mod connector;
pub use connector::NestedDozerConnector;
use dozer_ingestion_connector::dozer_types::thiserror::{self, Error};
use dozer_log::errors::{ReaderBuilderError, ReaderError};

#[derive(Error, Debug)]
enum NestedDozerConnectorError {
    #[error("Failed to connect to upstream dozer at {0}: {1:?}")]
    ConnectionError(String, #[source] dozer_services::tonic::transport::Error),

    #[error("Failed to query endpoints from upstream dozer app: {0}")]
    DescribeEndpointsError(#[source] dozer_services::tonic::Status),

    #[error(transparent)]
    ReaderError(#[from] ReaderError),

    #[error(transparent)]
    ReaderBuilderError(#[from] ReaderBuilderError),

    #[error("Column {0} not found")]
    ColumnNotFound(String),
}
