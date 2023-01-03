#![allow(clippy::enum_variant_names)]

use dozer_api::errors::GRPCError;
use dozer_cache::errors::CacheError;
use dozer_core::dag::errors::ExecutionError;
use dozer_ingestion::errors::ConnectorError;
use dozer_sql::pipeline::errors::PipelineError;
use dozer_types::crossbeam::channel::RecvError;
use dozer_types::errors::internal::BoxedError;
use dozer_types::thiserror::Error;
use dozer_types::{serde_yaml, thiserror};

#[derive(Error, Debug)]
pub enum OrchestrationError {
    #[error("Failed to write config yaml: {0:?}")]
    FailedToWriteConfigYaml(#[source] serde_yaml::Error),
    #[error("Failed to initialize. {0}[/api/generated,/cache] are not empty. Use -f to clean the directory and overwrite. Warning! there will be data loss.")]
    InitializationFailed(String),
    #[error("Failed to generate token: {0:?}")]
    GenerateTokenFailed(String),
    #[error("Failed to initialize api server: {0:?}")]
    ApiServerFailed(#[source] std::io::Error),
    #[error("Failed to initialize grpc server: {0:?}")]
    GrpcServerFailed(#[source] GRPCError),
    #[error("Failed to initialize cache in read only mode - {0:?}")]
    CacheInitFailed(#[source] CacheError),
    #[error("Ingestion message forwarding failed")]
    IngestionForwarderError,
    #[error(transparent)]
    InternalError(#[from] BoxedError),
    #[error(transparent)]
    ExecutionError(#[from] ExecutionError),
    #[error(transparent)]
    ConnectorError(#[from] ConnectorError),

    #[error(transparent)]
    CliError(#[from] CliError),

    #[error("Can't find the table name ({0:?}) in the sources provided.")]
    PortNotFound(String),

    #[error("Failed to initialize internal server")]
    InternalServerError,

    #[error("Failed to initialize SQL Statement as pipeline..")]
    SqlStatementFailed(#[source] PipelineError),

    #[error(transparent)]
    RecvError(#[from] RecvError),
}

#[derive(Error, Debug)]
pub enum CliError {
    #[error("Can't find the configuration file at: {0:?}")]
    FailedToLoadFile(String),
    #[error("Failed to parse dozer config: {0:?}")]
    FailedToParseYaml(#[source] BoxedError),
    #[error("Failed to validate dozer config: {0:?}")]
    FailedToParseValidateYaml(#[source] BoxedError),
}
