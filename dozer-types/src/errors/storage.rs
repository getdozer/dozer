#![allow(clippy::enum_variant_names)]
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataTypeError {
    #[error("Data type mismatch: {0}")]
    DataTypeMismatch(String),
}
