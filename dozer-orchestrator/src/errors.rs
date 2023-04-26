#![allow(clippy::enum_variant_names)]

use std::path::PathBuf;

use dozer_api::errors::{ApiError, GenerationError, GrpcError};
use dozer_cache::dozer_log::errors::SchemaError;
use dozer_cache::errors::CacheError;
use dozer_core::errors::ExecutionError;
use dozer_ingestion::errors::ConnectorError;
use dozer_sql::pipeline::errors::PipelineError;
use dozer_types::errors::internal::BoxedError;
use dozer_types::thiserror::Error;
use dozer_types::{serde_yaml, thiserror};

#[derive(Error, Debug)]
pub enum OrchestrationError {
    #[error("Failed to write config yaml: {0:?}")]
    FailedToWriteConfigYaml(#[source] serde_yaml::Error),
    #[error("Failed to initialize. {0} is not empty. Use -f to clean the directory and overwrite. Warning! there will be data loss.")]
    InitializationFailed(String),
    #[error("Failed to create directory {0:?}: {1}")]
    FailedToCreateDir(PathBuf, #[source] std::io::Error),
    #[error("Failed to write schema: {0}")]
    FailedToWriteSchema(#[source] SchemaError),
    #[error("Failed to generate proto files: {0:?}")]
    FailedToGenerateProtoFiles(#[from] GenerationError),
    #[error("Failed to initialize pipeline_dir. Is the path {0:?} accessible?: {1}")]
    PipelineDirectoryInitFailed(String, #[source] std::io::Error),
    #[error("Can't locate pipeline_dir. Have you run `dozer migrate`?")]
    PipelineDirectoryNotFound(String),
    #[error("Failed to generate token: {0:?}")]
    GenerateTokenFailed(String),
    #[error("Failed to deploy dozer application: {0:?}")]
    DeployFailed(#[from] DeployError),
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
    #[error("Source validation failed")]
    SourceValidationError,
    #[error("Pipeline validation failed")]
    PipelineValidationError,
    #[error("Table name specified in endpoint not found: {0:?}")]
    EndpointTableNotFound(String),
    #[error("Duplicate table name found: {0:?}")]
    DuplicateTable(String),
    #[error("No endpoints initialized in the config provided")]
    EmptyEndpoints,
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
    #[error("Failed to read line: {0}")]
    ReadlineError(#[from] rustyline::error::ReadlineError),
    #[error("File system error {0:?}: {1}")]
    FileSystem(PathBuf, #[source] std::io::Error),
    #[error("Failed to create tokio runtime: {0}")]
    FailedToCreateTokioRuntime(#[source] std::io::Error),
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
}

#[derive(Error, Debug)]
pub enum DeployError {
    #[error("Cannot read configuration: {0}")]
    CannotReadConfig(PathBuf, #[source] std::io::Error),
    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("Server error: {0}")]
    Server(#[from] tonic::Status),
}
