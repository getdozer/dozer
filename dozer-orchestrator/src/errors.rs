#![allow(clippy::enum_variant_names)]

use glob::{GlobError, PatternError};
use std::path::PathBuf;

use dozer_api::errors::{ApiError, AuthError, GenerationError, GrpcError};
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
    #[error("File system error {0:?}: {1}")]
    FileSystem(PathBuf, std::io::Error),
    #[error("Failed to find migration for endpoint {0}")]
    NoMigrationFound(String),
    #[error("Failed to migrate: {0}")]
    MigrateFailed(#[from] MigrationError),
    #[error("Failed to generate token: {0}")]
    GenerateTokenFailed(#[source] AuthError),
    #[error("Missing api config or security input")]
    MissingSecurityConfig,
    #[error("Cloud service error: {0}")]
    CloudError(#[from] CloudError),
    #[error("Failed to initialize api server: {0}")]
    ApiServerFailed(#[from] ApiError),
    #[error("Failed to initialize grpc server: {0}")]
    GrpcServerFailed(#[from] GrpcError),
    #[error("Failed to initialize internal server: {0}")]
    InternalServerFailed(#[source] tonic::transport::Error),
    #[error("{0}: Failed to initialize cache. Have you run `dozer migrate`?")]
    CacheInitFailed(#[source] CacheError),
    #[error("Failed to build cache from log")]
    CacheBuildFailed(#[source] CacheError),
    #[error("Internal thread panic: {0}")]
    JoinError(#[source] tokio::task::JoinError),
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
pub enum CloudError {
    #[error("Connection failed. Error: {0:?}")]
    ConnectionToCloudServiceError(#[from] tonic::transport::Error),

    #[error("Cloud service returned error: {0:?}")]
    CloudServiceError(#[from] tonic::Status),

    #[error("Cannot read configuration: {0:?}")]
    CannotReadConfig(PathBuf, #[source] std::io::Error),

    #[error("Wrong pattern of config files read glob: {0}")]
    WrongPatternOfConfigFilesGlob(#[from] PatternError),

    #[error("Cannot read file: {0}")]
    CannotReadFile(#[from] GlobError),
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("Got mismatching primary key for `{endpoint_name}`. Expected: `{expected:?}`, got: `{actual:?}`")]
    MismatchPrimaryKey {
        endpoint_name: String,
        expected: Vec<String>,
        actual: Vec<String>,
    },
    #[error("Field not found at position {0}")]
    FieldNotFound(String),
    #[error("File system error {0:?}: {1}")]
    FileSystem(PathBuf, std::io::Error),
    #[error("Cannot load existing schema: {0}")]
    CannotLoadExistingSchema(#[source] SchemaError),
    #[error("Cannot write schema: {0}")]
    CannotWriteSchema(#[source] SchemaError),
    #[error("Failed to generate proto files: {0:?}")]
    FailedToGenerateProtoFiles(#[from] GenerationError),
}
