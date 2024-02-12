#![allow(clippy::enum_variant_names)]

use glob::{GlobError, PatternError};
use std::io;
use std::path::PathBuf;
use std::string::FromUtf8Error;
use tonic::Code;
use tonic::Code::NotFound;

use crate::{
    errors::CloudError::{ApplicationNotFound, CloudServiceError},
    ui::{app::AppUIError, live::LiveError},
};

use dozer_core::errors::ExecutionError;
use dozer_sql::errors::PipelineError;
use dozer_types::{constants::LOCK_FILE, thiserror::Error};
use dozer_types::{errors::internal::BoxedError, serde_json};
use dozer_types::{serde_yaml, thiserror};

use crate::pipeline::connector_source::ConnectorSourceFactoryError;

pub fn map_tonic_error(e: tonic::Status) -> CloudError {
    if e.code() == NotFound && e.message() == "Failed to find app" {
        ApplicationNotFound
    } else {
        CloudServiceError(e)
    }
}

#[derive(Error, Debug)]
pub enum OrchestrationError {
    #[error("Failed to write config yaml: {0:?}")]
    FailedToWriteConfigYaml(#[source] serde_yaml::Error),
    #[error("File system error {0:?}: {1}")]
    FileSystem(PathBuf, std::io::Error),
    #[error("Failed to find any build")]
    NoBuildFound,
    #[error("Failed to login: {0}")]
    CloudLoginFailed(#[from] CloudLoginError),
    #[error("Credential Error: {0}")]
    CredentialError(#[from] CloudCredentialError),
    #[error("Failed to build: {0}")]
    BuildFailed(#[from] BuildError),
    #[error("Missing api config or security input")]
    MissingSecurityConfig,
    #[error(transparent)]
    CloudError(#[from] CloudError),
    #[error("Failed to server REST API: {0}")]
    RestServeFailed(#[source] std::io::Error),
    #[error("Failed to server gRPC API: {0:?}")]
    GrpcServeFailed(#[source] tonic::transport::Error),
    #[error("Failed to server pgwire: {0}")]
    PGWireServerFailed(#[source] std::io::Error),
    #[error("Cache {0} has reached its maximum size. Try to increase `cache_max_map_size` in the config.")]
    CacheFull(String),
    #[error("Internal thread panic: {0}")]
    JoinError(#[source] tokio::task::JoinError),
    #[error("Connector source factory error: {0}")]
    ConnectorSourceFactory(#[from] ConnectorSourceFactoryError),
    #[error(transparent)]
    ExecutionError(#[from] ExecutionError),
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
    CloudContextError(#[from] CloudContextError),
    #[error("Failed to read organisation name. Error: {0}")]
    FailedToReadOrganisationName(#[source] io::Error),
    #[error(transparent)]
    LiveError(#[from] LiveError),
    #[error(transparent)]
    AppUIError(#[from] AppUIError),
    #[error("{LOCK_FILE} is out of date")]
    LockedOutdatedLockfile,
    #[error("{LOCK_FILE} does not exist. `--locked` requires a lock file.")]
    LockedNoLockFile,
    #[error("Command was aborted")]
    Aborted,
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

#[derive(Error, Debug)]
pub enum CloudError {
    #[error("Connection failed. Error: {0:?}")]
    ConnectionToCloudServiceError(#[from] tonic::transport::Error),

    #[error("Cloud service returned error: {}", map_status_error_message(.0.clone()))]
    CloudServiceError(#[from] tonic::Status),

    #[error("GRPC request failed, error: {} (GRPC status {})", .0.message(), .0.code())]
    GRPCCallError(#[source] tonic::Status),

    #[error(transparent)]
    CloudCredentialError(#[from] CloudCredentialError),

    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    CloudContextError(#[from] CloudContextError),

    #[error(transparent)]
    ConfigCombineError(#[from] ConfigCombineError),

    #[error("Application not found")]
    ApplicationNotFound,

    #[error("{LOCK_FILE} not found. Run `dozer build` before deploying, or pass '--no-lock'.")]
    LockfileNotFound,
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

    #[error("Cannot read configuration: {0:?}. Error: {1:?}")]
    CannotReadConfig(PathBuf, #[source] std::io::Error),

    #[error("Wrong pattern of config files read glob: {0}")]
    WrongPatternOfConfigFilesGlob(#[from] PatternError),

    #[error("Cannot read file: {0}")]
    CannotReadFile(#[from] GlobError),

    #[error("Cannot serialize config to string: {0}")]
    CannotSerializeToString(#[source] serde_yaml::Error),

    #[error("SQL is not a string type")]
    SqlIsNotStringType,

    #[error("Failed to read config to string")]
    CannotReadUtf8String(#[from] FromUtf8Error),
}

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("Endpoint {0} not found in DAG")]
    MissingEndpoint(String),
    #[error("Connection {0} found in DAG but not in config")]
    MissingConnection(String),
    #[error("Got mismatching primary key for `{table_name}`. Expected: `{expected:?}`, got: `{actual:?}`")]
    MismatchPrimaryKey {
        table_name: String,
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
}

#[derive(Debug, Error)]
pub enum CloudLoginError {
    #[error("Tonic error: {0}")]
    TonicError(#[from] tonic::Status),

    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("HttpRequest error: {0}")]
    HttpRequestError(#[from] reqwest::Error),

    #[error(transparent)]
    SerializationError(#[from] dozer_types::serde_json::Error),

    #[error("Failed to read input: {0}")]
    InputError(#[from] std::io::Error),

    #[error(transparent)]
    CloudCredentialError(#[from] CloudCredentialError),

    #[error("Organisation not found")]
    OrganisationNotFound,
}

#[derive(Debug, Error)]
pub enum CloudCredentialError {
    #[error(transparent)]
    SerializationError(#[from] dozer_types::serde_yaml::Error),

    #[error(transparent)]
    JsonSerializationError(#[from] dozer_types::serde_json::Error),
    #[error("Failed to create home directory: {0}")]
    FailedToCreateDirectory(#[from] std::io::Error),

    #[error("HttpRequest error: {0}")]
    HttpRequestError(#[from] reqwest::Error),

    #[error("Missing credentials.yaml file - Please try to login again")]
    MissingCredentialFile,
    #[error("There's no profile with given name - Please try to login again")]
    MissingProfile,
    #[error("{0}")]
    LoginError(String),
}

#[derive(Debug, Error)]
pub enum CloudContextError {
    #[error("Failed to create access directory: {0}")]
    FailedToAccessDirectory(#[from] std::io::Error),

    #[error("Failed to get current directory path")]
    FailedToGetDirectoryPath,

    #[error("App id not found in configuration. You need to run \"deploy\" or \"set-app\" first")]
    AppIdNotFound,

    #[error("App id already exists. If you want to create a new app, please remove your cloud configuration")]
    AppIdAlreadyExists(String),
}

fn map_status_error_message(status: tonic::Status) -> String {
    match status.code() {
        Code::PermissionDenied => {
            "Permission denied. Please check your credentials and try again.".to_string()
        }
        _ => status.message().to_string(),
    }
}
