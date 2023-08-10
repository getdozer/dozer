use crate::errors::CliError;
use dozer_sql::pipeline::errors::PipelineError;
use dozer_types::errors::internal::BoxedError;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;
use rusoto_core::{request::TlsError, RusotoError};
use rusoto_s3::GetObjectError;
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
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("String error: {0}")]
    StringError(String),
    #[error("S3 error: {0}")]
    S3Error(RusotoError<GetObjectError>),
    #[error("TLS error: {0}")]
    TlsError(#[from] TlsError),
    #[error(transparent)]
    PipelineError(#[from] PipelineError),
    #[error("Zip error: {0}")]
    ZipError(#[from] ZipError),
}

impl From<&'static str> for LiveError {
    fn from(error: &'static str) -> Self {
        LiveError::StringError(error.to_owned())
    }
}

impl From<RusotoError<GetObjectError>> for LiveError {
    fn from(error: RusotoError<GetObjectError>) -> Self {
        LiveError::S3Error(error)
    }
}
