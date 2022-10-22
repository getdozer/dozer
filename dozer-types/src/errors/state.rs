#![allow(clippy::enum_variant_names)]
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StateStoreError {
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("invalid operation")]
    InvalidOperation,
}
