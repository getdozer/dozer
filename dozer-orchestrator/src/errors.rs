#![allow(clippy::enum_variant_names)]

use std::path::PathBuf;

use dozer_api::errors::{ApiError, GenerationError, GrpcError};
use dozer_cache::errors::CacheError;
use dozer_core::errors::ExecutionError;
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
    #[error("Failed to generate proto files: {0:?}")]
    FailedToGenerateProtoFiles(#[from] GenerationError),
    #[error("Failed to initialize pipeline_dir. Is the path {0:?} accessible?: {1}")]
    PipelineDirectoryInitFailed(String, #[source] std::io::Error),
    #[error("Can't locate pipeline_dir. Have you run `dozer migrate`?")]
    PipelineDirectoryNotFound(String),
    #[error("Failed to generate token: {0:?}")]
    GenerateTokenFailed(String),
    #[error("Failed to deploy dozer application: {0:?}")]
    DeployFailed(DeployError),
    #[error("Failed to initialize api server: {0}")]
    ApiServerFailed(#[from] ApiError),
    #[error("Failed to initialize grpc server: {0}")]
    GrpcServerFailed(#[from] GrpcError),
    #[error("Failed to initialize internal server: {0}")]
    InternalServerFailed(#[source] tonic::transport::Error),
    #[error("Failed to initialize cache: {0}")]
    RwCacheInitFailed(#[source] CacheError),
    #[error("{0}: Failed to initialize read only cache. Have you run `dozer migrate`?")]
    RoCacheInitFailed(#[source] CacheError),
    #[error("Failed to build cache from log")]
    CacheBuildFailed(#[source] CacheError),
    #[error(transparent)]
    InternalError(#[from] BoxedError),
    #[error(transparent)]
    ExecutionError(#[from] ExecutionError),
    #[error(transparent)]
    ConnectorError(#[from] ConnectorError),
    #[error(transparent)]
    PipelineError(#[from] PipelineError),
    #[error(transparent)]
    CliError(#[from] CliError),
    #[error("Failed to receive server handle from grpc server: {0}")]
    GrpcServerHandleError(#[source] RecvError),
    #[error("Source validation failed")]
    SourceValidationError,
    #[error("Pipeline validation failed")]
    PipelineValidationError,
    #[error("Table name specified in endpoint not found: {0:?}")]
    EndpointTableNotFound(String),
    #[error("Duplicate table name found: {0:?}")]
    DuplicateTable(String),
    #[error("Configuration Error: {0:?}")]
    ConfigError(String),
    #[error("Loading Schema failed: {0:?}")]
    SchemaLoadFailed(#[source] CacheError),
    #[error("Schemas not found in Path specified {0:?}")]
    SchemasNotInitializedPath(PathBuf),
    #[error("Cannot convert Schema in Path specified {0:?}")]
    DeserializeSchema(PathBuf),
    #[error("Got mismatching primary key for `{endpoint_name}`. Expected: `{expected:?}`, got: `{actual:?}`")]
    MismatchPrimaryKey {
        endpoint_name: String,
        expected: Vec<String>,
        actual: Vec<String>,
    },
    #[error("Field not found at position {0}")]
    FieldNotFound(String),
}

#[derive(Error, Debug)]
pub enum CliError {
    #[error("Can't find the configuration file at: {0:?}")]
    FailedToLoadFile(String),
    #[error("Unknown Command: {0:?}")]
    UnknownCommand(String),
    #[error("Failed to parse dozer config: {0:?}")]
    FailedToParseYaml(#[source] BoxedError),
    #[error("Failed to validate dozer config: {0:?}")]
    FailedToParseValidateYaml(#[source] BoxedError),
    #[error(transparent)]
    ReadlineError(#[from] rustyline::error::ReadlineError),
    #[error(transparent)]
    InternalError(#[from] BoxedError),
    #[error(transparent)]
    TerminalError(#[from] crossterm::ErrorKind),
}

#[derive(Error, Debug)]
pub enum DeployError {
    #[error("Invalid deployment target url")]
    InvalidTargetUrl,
}
