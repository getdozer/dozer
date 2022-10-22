#![allow(clippy::enum_variant_names)]
use crate::core::node::{NodeHandle, PortHandle};
use crate::errors::internal::BoxedError;
use crate::errors::state::StateStoreError;
use crate::errors::types::TypeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("Invalid port handle {0}")]
    InvalidPortHandle(PortHandle),
    #[error("Invalid operation received {0}")]
    InvalidOperation(String),
    #[error("Schema not initialized")]
    SchemaNotInitialized,
    #[error("The node {0} does not have any input")]
    MissingNodeInput(NodeHandle),
    #[error("The node {0} does not have any output")]
    MissingNodeOutput(NodeHandle),
    #[error("The node type is not valid")]
    InvalidNodeType,
    #[error("The node {0} is not valid")]
    InvalidNode(NodeHandle),
    #[error("{0}")]
    InternalStringError(String),
    #[error(transparent)]
    InternalTypeError(#[from] TypeError),
    #[error(transparent)]
    InternalStateStoreError(#[from] StateStoreError),
    #[error(transparent)]
    InternalError(#[from] BoxedError),
}
