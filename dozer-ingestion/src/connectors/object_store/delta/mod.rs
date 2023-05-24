use crate::errors::ConnectorError;

mod reader;
pub(crate) mod schema_helper;

type ConnectorResult<T> = Result<T, ConnectorError>;
