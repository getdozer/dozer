use geo::vincenty_distance::FailedToConvergeError;
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
    #[error("Invalid field value: {0}")]
    InvalidFieldValue(String),
    #[error("Invalid timestamp")]
    InvalidTimestamp,
    #[error("Ambiguous timestamp")]
    AmbiguousTimestamp,
    #[error("Serialization failed: {0}")]
    SerializationError(#[source] SerializationError),
    #[error("Failed to parse the field: {0}")]
    DeserializationError(#[source] DeserializationError),
    #[error("Failed to calculate distance: {0}")]
    DistanceCalculationError(#[source] FailedToConvergeError),
}

#[derive(Error, Debug)]
pub enum SerializationError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    #[error(transparent)]
    Custom(#[from] BoxedError),
}

#[derive(Error, Debug)]
pub enum DeserializationError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    #[error(transparent)]
    Custom(#[from] BoxedError),
    #[error("Empty input")]
    EmptyInput,
    #[error("Unrecognised field type : {0}")]
    UnrecognisedFieldType(u8),
    #[error("Bad data length")]
    BadDataLength,
    #[error(transparent)]
    BadDateFormat(#[from] chrono::ParseError),
    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),
}
