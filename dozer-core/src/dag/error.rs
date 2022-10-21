#![allow(clippy::enum_variant_names)]
use crate::dag::dag::{NodeHandle, PortHandle};
use crate::dag::mt_executor::ExecutorOperation;
use crate::state::error::StateStoreError;
use crossbeam::channel::{RecvError, SendError};
use dozer_types::types::TypeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("Invalid port handle {0}")]
    InvalidPortHandle(PortHandle),
    #[error("Invalid operation received {0}")]
    InvalidOperation(ExecutorOperation),
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
    InternalChannelSendError(#[from] SendError<ExecutorOperation>),
    #[error(transparent)]
    InternalChannelRecvError(#[from] RecvError),
    #[error(transparent)]
    InternalTypeError(#[from] TypeError),
    #[error(transparent)]
    InternalStateStoreError(#[from] StateStoreError),
    #[error(transparent)]
    InternalError(#[from] Box<dyn std::error::Error + Send>),
}
