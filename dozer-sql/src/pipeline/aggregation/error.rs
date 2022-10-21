#![allow(clippy::enum_variant_names)]
use dozer_core::state::error::StateStoreError;
use dozer_types::types::TypeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AggregatorError {
    #[error(transparent)]
    InternalStateStoreError(#[from] StateStoreError),
    #[error(transparent)]
    InternalTypeError(#[from] TypeError),
    #[error("Invalid operand type for function: {0}()")]
    InvalidOperandType(String),
}
