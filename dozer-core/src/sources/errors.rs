use crate::sources::subscriptions::{
    PipelineId, RelationSourceName, SourceName, SourceRelationName,
};
use dozer_types::errors::internal::BoxedError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SourceError {
    #[error("Source {0}.{1} does not exist")]
    InvalidSource(SourceName, SourceRelationName),
    #[error("Unable to forward message to pipelines")]
    ForwardError(Vec<PipelineId>),
}
