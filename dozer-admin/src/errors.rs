#![allow(clippy::enum_variant_names)]
use dozer_types::errors::internal::BoxedError;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;

#[derive(Error, Debug)]
pub enum AdminError {
    #[error("Couldnt read file")]
    FailedToLoadFile(#[source] std::io::Error),
    #[error("Failed to parse admin-dozer config..")]
    FailedToParseYaml(#[source] BoxedError),
    #[error("Failed to parse dozer config..")]
    FailedToParseDozerConfig(#[source] BoxedError),
    #[error("Failed to run cli")]
    FailedToRunCli(#[source] Box<dyn std::error::Error>),
}
