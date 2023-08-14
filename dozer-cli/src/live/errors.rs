use crate::errors::CliError;
use dozer_sql::pipeline::errors::PipelineError;
use dozer_types::errors::internal::BoxedError;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;
use zip::result::ZipError;

#[derive(Error, Debug)]
pub enum LiveError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Notify error: {0}")]
    Notify(#[from] notify::Error),
    #[error(transparent)]
    CliError(#[from] CliError),
    #[error(transparent)]
    BuildError(#[from] BoxedError),
    #[error("Dozer is not initialized")]
    NotInitialized,
    #[error("Error in initializing live server: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("Error in reading or extracting from Zip file: {0}")]
    ZipError(#[from] ZipError),
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Cannot start ui server: {0}")]
    CannotStartUiServer(#[source] std::io::Error),

    #[error(transparent)]
    PipelineError(#[from] PipelineError),
}
