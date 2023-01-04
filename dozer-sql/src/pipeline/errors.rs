#![allow(clippy::enum_variant_names)]

use dozer_core::dag::errors::ExecutionError;
use dozer_core::storage::errors::StorageError;
use dozer_types::errors::internal::BoxedError;
use dozer_types::errors::types::TypeError;
use dozer_types::types::{Field, FieldType};
use std::fmt::{Display, Formatter};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct FieldTypes {
    types: Vec<FieldType>,
}

impl FieldTypes {
    pub fn new(types: Vec<FieldType>) -> Self {
        Self { types }
    }
}

impl Display for FieldTypes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str_list: Vec<String> = self.types.iter().map(|e| e.to_string()).collect();
        f.write_str(str_list.join(", ").as_str())
    }
}

#[derive(Error, Debug)]
pub enum PipelineError {
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
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
    #[error("Invalid relation")]
    InvalidRelation,
    #[error("Invalid relation")]
    DataTypeMismatch,
    #[error("Invalid argument for function {0}(): argument: {1}, index: {2}")]
    InvalidFunctionArgument(String, Field, usize),
    #[error("Not enough arguments for function {0}()")]
    NotEnoughArguments(String),
    #[error(
        "Invalid argument type for function {0}(): type: {1}, expected types: {2}, index: {3}"
    )]
    InvalidFunctionArgumentType(String, FieldType, FieldTypes, usize),

    // Error forwarding
    #[error(transparent)]
    InternalStorageError(#[from] StorageError),
    #[error(transparent)]
    InternalTypeError(#[from] TypeError),
    #[error(transparent)]
    InternalExecutionError(#[from] ExecutionError),
    #[error(transparent)]
    InternalError(#[from] BoxedError),
}
