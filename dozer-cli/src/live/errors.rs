use dozer_sql::pipeline::errors::PipelineError;
use dozer_types::errors::internal::BoxedError;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;

use crate::errors::CliError;

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

    #[error(transparent)]
    PipelineError(#[from] PipelineError),
}
