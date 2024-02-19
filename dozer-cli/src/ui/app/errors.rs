use crate::errors::{BuildError, CliError, OrchestrationError};
use crate::ui::downloader::DownloaderError;
use dozer_core::errors::ExecutionError;
use dozer_sql::errors::PipelineError;

use dozer_types::thiserror;
use dozer_types::thiserror::Error;
use zip::result::ZipError;

#[derive(Error, Debug)]
pub enum AppUIError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Notify error: {0}")]
    Notify(#[from] notify::Error),
    #[error(transparent)]
    CliError(#[from] CliError),

    #[error("Cannot pull docker image: {0}")]
    CannotPullDockerImage(String),
    #[error("Cannot run docker image: {0}")]
    CannotRunDockerImage(String),
    #[error("Docker not installed")]
    DockerNotInstalled,
    #[error("Cannot stop docker container: {0}")]
    CannotStopDockerContainer(String),
    #[error("Cannot remove docker container: {0}")]
    CannotRemoveDockerContainer(String),

    #[error("Dozer is not initialized")]
    NotInitialized,
    #[error("Connection {0} not found")]
    ConnectionNotFound(String),
    #[error("Sink {0} not found")]
    SinkNotFound(String),
    #[error("Error in initializing app ui server: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("Error in reading or extracting from Zip file: {0}")]
    ZipError(#[from] ZipError),
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Cannot start ui server: {0}")]
    CannotStartUiServer(#[source] std::io::Error),

    #[error(transparent)]
    Build(#[from] BuildError),
    #[error(transparent)]
    PipelineError(#[from] PipelineError),
    #[error(transparent)]
    ExecutionError(#[from] ExecutionError),
    #[error(transparent)]
    OrchestrationError(Box<OrchestrationError>),

    #[error(transparent)]
    DownloaderError(#[from] DownloaderError),
}

impl From<OrchestrationError> for AppUIError {
    fn from(error: OrchestrationError) -> Self {
        AppUIError::OrchestrationError(Box::new(error))
    }
}
