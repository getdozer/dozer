#![allow(clippy::enum_variant_names)]
use crate::errors::internal::BoxedError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    #[error("Unable to deserialize type: {} - Reason: {}", typ, reason.to_string())]
    DeserializationError { typ: String, reason: BoxedError },
    #[error("Unable to serialize type: {} - Reason: {}", typ, reason.to_string())]
    SerializationError { typ: String, reason: BoxedError },

    // Error forwarding
    #[error(transparent)]
    InternalError(#[from] BoxedError),
}
