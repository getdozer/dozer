use thiserror::Error;

use super::internal::BoxedError;

#[derive(Error, Debug)]
pub enum TypeError {
    #[error("Invalid field index: {0}")]
    InvalidFieldIndex(usize),
    #[error("Invalid field name: {0}")]
    InvalidFieldName(String),
    #[error("Invalid field type")]
    InvalidFieldType,
    #[error("Failed to serialise the field")]
    SerializationError(#[source] SerializationError),
    #[error("Failed to deserialise the field")]
    DeserializationError(#[source] DeserializationError),
}

#[derive(Error, Debug)]
pub enum SerializationError {
    #[error("Failed serialising json field")]
    Json(#[source] serde_json::Error),
    #[error("Failed serialising bincode field")]
    Bincode(#[source] bincode::Error),
    #[error("Failed serialising custom field")]
    Custom(#[source] BoxedError),
}

#[derive(Error, Debug)]
pub enum DeserializationError {
    #[error("Failed deserialising json field")]
    Json(#[source] serde_json::Error),
    #[error("Failed deserialising bincode field")]
    Bincode(#[source] bincode::Error),
    #[error("Failed deserialising custom field")]
    Custom(#[source] BoxedError),
}
