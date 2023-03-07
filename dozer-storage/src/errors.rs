#![allow(clippy::enum_variant_names)]
use dozer_types::errors::internal::BoxedError;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Unable to open or create database at location: {0}")]
    OpenOrCreateError(String),
    #[error("Unable to deserialize type: {} - Reason: {}", typ, reason.to_string())]
    DeserializationError {
        typ: &'static str,
        reason: BoxedError,
    },
    #[error("Unable to serialize type: {} - Reason: {}", typ, reason.to_string())]
    SerializationError {
        typ: &'static str,
        reason: BoxedError,
    },
    #[error("Invalid dataset: {0}")]
    InvalidDatasetIdentifier(String),
    #[error("Invalid key: {0}")]
    InvalidKey(String),
    #[error("Invalid record")]
    InvalidRecord,

    // Error forwarding
    #[error("Lmdb error: {0}")]
    Lmdb(#[from] lmdb::Error),
}
