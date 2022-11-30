use crate::sources::subscriptions::{
    PipelineId, RelationSourceName, RelationUniqueName, SourceName, SourceRelationName,
};
use dozer_types::errors::internal::BoxedError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SourceError {
    #[error("Relation {0} does not exist")]
    InvalidRelation(RelationUniqueName),
    #[error("Unable to forward message to pipelines")]
    ForwardError(Vec<PipelineId>),
}
