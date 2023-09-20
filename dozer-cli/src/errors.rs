#![allow(clippy::enum_variant_names)]

use crate::live::LiveError;
use dozer_api::errors::{ApiInitError, AuthError, GenerationError, GrpcError};
use dozer_cache::dozer_log::storage;
use dozer_cache::errors::CacheError;
use dozer_core::errors::ExecutionError;
use dozer_ingestion::errors::ConnectorError;
use dozer_sql::errors::PipelineError;
use dozer_types::{constants::LOCK_FILE, thiserror::Error};
use dozer_types::{errors::internal::BoxedError, serde_json};
use dozer_types::{serde_yaml, thiserror};
use glob::{GlobError, PatternError};
use std::path::PathBuf;

use crate::pipeline::connector_source::ConnectorSourceFactoryError;

#[derive(Error, Debug)]
pub enum OrchestrationError {
    #[error("Failed to write config yaml: {0:?}")]
    FailedToWriteConfigYaml(#[source] serde_yaml::Error),
    #[error("File system error {0:?}: {1}")]
    FileSystem(PathBuf, std::io::Error),
    #[error("Failed to find any build")]
    NoBuildFound,
    #[error("Failed to create log: {0}")]
    CreateLog(#[from] dozer_cache::dozer_log::replication::Error),
    #[error("Failed to build: {0}")]
    BuildFailed(#[from] BuildError),
    #[error("Failed to generate token: {0}")]
    GenerateTokenFailed(#[source] AuthError),
    #[error("Missing api config or security input")]
    MissingSecurityConfig,
    #[error("Failed to initialize api server: {0}")]
    ApiInitFailed(#[from] ApiInitError),
    #[error("Failed to server REST API: {0}")]
    RestServeFailed(#[source] std::io::Error),
    #[error("Failed to server gRPC API: {0:?}")]
    GrpcServeFailed(#[source] tonic::transport::Error),
    #[error("Failed to initialize internal server: {0}")]
    InternalServerFailed(#[source] GrpcError),
    #[error("{0}: Failed to initialize cache. Have you run `dozer build`?")]
    CacheInitFailed(#[source] CacheError),
    #[error("Failed to build cache {0} from log: {1}")]
    CacheBuildFailed(String, #[source] CacheError),
    #[error("Cache {0} has reached its maximum size. Try to increase `cache_max_map_size` in the config.")]
    CacheFull(String),
    #[error("Internal thread panic: {0}")]
    JoinError(#[source] tokio::task::JoinError),
    #[error("Connector source factory error: {0}")]
    ConnectorSourceFactory(#[from] ConnectorSourceFactoryError),
    #[error(transparent)]
    ExecutionError(#[from] ExecutionError),
    #[error(transparent)]
    ConnectorError(#[from] ConnectorError),
    #[error(transparent)]
    PipelineError(#[from] PipelineError),
    #[error(transparent)]
    CliError(#[from] CliError),
    #[error("table_name: {0:?} not found in any of the connections")]
    SourceValidationError(String),
    #[error("connection: {0:?} not found")]
    ConnectionNotFound(String),
    #[error("Pipeline validation failed")]
    PipelineValidationError,
    #[error("Output table {0} not used in any endpoint")]
    OutputTableNotUsed(String),
    #[error("Table name specified in endpoint not found: {0:?}")]
    EndpointTableNotFound(String),
    #[error("No endpoints initialized in the config provided")]
    EmptyEndpoints,
    #[error(transparent)]
    LiveError(#[from] LiveError),
    #[error("{LOCK_FILE} is out of date")]
    LockedOutdatedLockfile,
    #[error("{LOCK_FILE} does not exist. `--locked` requires a lock file.")]
    LockedNoLockFile,
    #[cfg(feature = "cloud")]
    #[error(transparent)]
    CloudError(#[from] dozer_cloud_client::errors::CloudError),
}

#[derive(Error, Debug)]
pub enum CliError {
    #[error("Configuration file path not provided")]
    ConfigurationFilePathNotProvided,
    #[error("Can't find the configuration file(s) at: {0:?}")]
    FailedToFindConfigurationFiles(String),
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
    #[error(transparent)]
    ConfigCombineError(#[from] ConfigCombineError),
    #[error("Failed to serialize config to json: {0}")]
    SerializeConfigToJson(#[source] serde_json::Error),
    #[error("Missing config options to be overridden: {0}")]
    MissingConfigOverride(String),
    #[error("Failed to deserialize config from json: {0}")]
    DeserializeConfigFromJson(#[source] serde_json::Error),
    // Generic IO error
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum ConfigCombineError {
    #[error("Failed to parse yaml file {0}: {1}")]
    ParseYaml(String, #[source] serde_yaml::Error),

    #[error("Cannot merge yaml value {from:?} to {to:?}")]
    CannotMerge {
        from: serde_yaml::Value,
        to: serde_yaml::Value,
    },

    #[error("Failed to parse config: {0}")]
    ParseConfig(#[source] serde_yaml::Error),

    #[error("Cannot read configuration: {0:?}")]
    CannotReadConfig(PathBuf, #[source] std::io::Error),

    #[error("Wrong pattern of config files read glob: {0}")]
    WrongPatternOfConfigFilesGlob(#[from] PatternError),

    #[error("Cannot read file: {0}")]
    CannotReadFile(#[from] GlobError),

    #[error("Cannot serialize config to string: {0}")]
    CannotSerializeToString(#[source] serde_yaml::Error),

    #[error("SQL is not a string type")]
    SqlIsNotStringType,
}

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("Endpoint {0} not found in DAG")]
    MissingEndpoint(String),
    #[error("Connection {0} found in DAG but not in config")]
    MissingConnection(String),
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
    #[error(
        "Failed to load existing contract: {0}. You have to run a force build: `dozer build --force`."
    )]
    FailedToLoadExistingContract(#[source] serde_json::Error),
    #[error("Serde json error: {0}")]
    SerdeJson(#[source] serde_json::Error),
    #[error("Failed to generate proto files: {0:?}")]
    FailedToGenerateProtoFiles(#[from] GenerationError),
    #[error("Storage error: {0}")]
    Storage(#[from] storage::Error),
}
