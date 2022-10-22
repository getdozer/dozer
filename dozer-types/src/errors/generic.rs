#![allow(clippy::enum_variant_names)]
use thiserror::Error;

pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[macro_export]
macro_rules! internal_err {
    ($stmt:expr) => {
        $stmt.map_err(|e| InternalError(Box::new(e)))
    };
}

#[derive(Error, Debug)]
pub enum DataTypeError {
    #[error("Data type mismatch: {0}")]
    DataTypeMismatch(String),
}
