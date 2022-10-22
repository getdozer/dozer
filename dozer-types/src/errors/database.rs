#![allow(clippy::enum_variant_names)]
use crate::errors::internal::BoxedError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    #[error("Unable to open or create database at location: {0}")]
    OpenOrCreateError(String),
    #[error(transparent)]
    InternalError(#[from] BoxedError),
}
