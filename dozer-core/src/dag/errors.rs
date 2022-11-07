#![allow(clippy::enum_variant_names)]
use crate::dag::node::{NodeHandle, PortHandle};
use crate::storage::lmdb_sys::LmdbError;
use dozer_types::errors::internal::BoxedError;
use dozer_types::errors::types::TypeError;
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
    #[error("The database is invalid")]
    InvalidDatabase,

    #[error("Field not found at position {0}")]
    FieldNotFound(String),

    // Error forwarders
    #[error(transparent)]
    InternalTypeError(#[from] TypeError),
    #[error(transparent)]
    InternalDatabaseError(#[from] LmdbError),
    #[error(transparent)]
    InternalError(#[from] BoxedError),

    // to remove
    #[error("{0}")]
    InternalStringError(String),
}
