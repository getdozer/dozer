#![allow(clippy::enum_variant_names)]
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExpressionError {
    #[error("invalid operand type for operator or function {0}")]
    InvalidOperandType(String),
    #[error("invalid input type. Reason: {0}")]
    InvalidInputType(String),
    #[error("invalid function {0}")]
    InvalidFunction(String),
}
