#![allow(clippy::enum_variant_names)]

use thiserror::Error;

use super::{connector::ConnectorError, execution::ExecutionError};

#[derive(Error, Debug)]
pub enum OrchestrationError {
    #[error("Failed to initialize dozer config..")]
    InitializationFailed,
    #[error("Failed to initialize api server..")]
    ApiServerFailed,
    #[error("Ingestion message forwarding failed")]
    IngestionForwarderError,
    #[error(transparent)]
    ExecutionError(#[from] ExecutionError),
    #[error(transparent)]
    ConnectorError(#[from] ConnectorError),
}
