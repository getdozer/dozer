use crate::sources::subscriptions::{PipelineId, RelationName, RelationSourceName, SourceName};
use dozer_types::errors::internal::BoxedError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SourceError {
    #[error("Source {0}.{1} does not exist")]
    InvalidSource(SourceName, RelationName),
    #[error("Unable to forward message to pipelines")]
    ForwardError(Vec<PipelineId>),
}
