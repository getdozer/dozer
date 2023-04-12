#![allow(clippy::enum_variant_names)]
use dozer_types::errors::internal::BoxedError;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;

#[derive(Error, Debug)]
pub enum AdminError {
    #[error("Couldnt read file: {0:?}")]
    FailedToLoadFile(#[source] std::io::Error),
    #[error("Failed to parse admin-dozer config: {0:?}")]
    FailedToParseYaml(#[source] BoxedError),
    #[error("Failed to parse dozer config: {0:?}")]
    FailedToParseDozerConfig(#[source] BoxedError),
    #[error("Failed to run cli: {0:?}")]
    FailedToRunCli(#[source] Box<dyn std::error::Error>),
}
