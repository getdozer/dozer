#![allow(clippy::enum_variant_names)]
use dozer_core::state::error::StateStoreError;
use dozer_types::types::TypeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PipelineError {
    #[error(transparent)]
    InternalStateStoreError(#[from] StateStoreError),
    #[error(transparent)]
    InternalTypeError(#[from] TypeError),
    #[error("Invalid operand type for function: {0}()")]
    InvalidOperandType(String),
    #[error("Invalid input type. Reason: {0}")]
    InvalidInputType(String),
    #[error("Invalid function: {0}")]
    InvalidFunction(String),
    #[error("Invalid operator: {0}")]
    InvalidOperator(String),
    #[error("Invalid expression: {0}")]
    InvalidExpression(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Invalid value: {0}")]
    InvalidValue(String),
    #[error("Invalid query")]
    InvalidQuery,
    #[error("Invalid relation")]
    InvalidRelation,
}
