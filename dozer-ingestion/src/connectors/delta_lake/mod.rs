use crate::errors::ConnectorError;

mod connector;
mod reader;
mod schema_helper;
mod test;

pub use connector::DeltaLakeConnector;

type ConnectorResult<T> = Result<T, ConnectorError>;
