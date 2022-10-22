#![allow(clippy::enum_variant_names)]
use crate::core::node::{NodeHandle, PortHandle};
use crate::errors::database::DatabaseError;
use crate::errors::internal::BoxedError;
use crate::errors::pipeline::PipelineError;
use crate::errors::types::TypeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("Invalid port handle: {0}")]
    InvalidPortHandle(PortHandle),
    #[error("Invalid node handle: {0}")]
    InvalidNodeHandle(NodeHandle),
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    #[error("Schema not initialized")]
    SchemaNotInitialized,
    #[error("The node {0} does not have any input")]
    MissingNodeInput(NodeHandle),
    #[error("The node {0} does not have any output")]
    MissingNodeOutput(NodeHandle),
    #[error("The node type is invalid")]
    InvalidNodeType,

    // Error forwarders
    #[error(transparent)]
    InternalPipelineError(#[from] PipelineError),
    #[error(transparent)]
    InternalTypeError(#[from] TypeError),
    #[error(transparent)]
    InternalDatabaseError(#[from] DatabaseError),
    #[error(transparent)]
    InternalError(#[from] BoxedError),

    // to remove
    #[error("{0}")]
    InternalStringError(String),
}
