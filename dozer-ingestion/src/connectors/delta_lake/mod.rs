use crate::errors::ConnectorError;

mod connector;
mod schema_helper;
mod reader;


pub use connector::DeltaLakeConnector;


type ConnectorResult<T> = Result<T, ConnectorError>;