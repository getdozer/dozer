#![allow(clippy::enum_variant_names)]

use thiserror::Error;

use super::{connector::ConnectorError, execution::ExecutionError, internal::BoxedError};

#[derive(Error, Debug)]
pub enum OrchestrationError {
    #[error("Couldnt read file")]
    FailedToLoadFile(#[source] std::io::Error),
    #[error("Failed to parse dozer config..")]
    FailedToParseYaml(#[source] BoxedError),
    #[error("Failed to initialize dozer config..")]
    InitializationFailed,
    #[error("Failed to initialize api server..")]
    ApiServerFailed,
    #[error("Failed to initialize schema registry..")]
    SchemaServerFailed,
    #[error("Ingestion message forwarding failed")]
    IngestionForwarderError,
    #[error(transparent)]
    ExecutionError(#[from] ExecutionError),
    #[error(transparent)]
    ConnectorError(#[from] ConnectorError),
}
