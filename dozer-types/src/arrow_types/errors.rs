use arrow::error::ArrowError;
use thiserror::Error;

use crate::errors::internal::BoxedError;

#[derive(Error, Debug)]
pub enum ArrowConversionError {
    #[error(transparent)]
    ArrowError(#[from] ArrowError),

    #[error(transparent)]
    FromArrowError(#[from] FromArrowError),

    #[error(transparent)]
    BoxedError(#[from] BoxedError),
}

#[derive(Error, Debug)]
pub enum FromArrowError {
    #[error("Unsupported type of \"{0}\" field")]
    FieldTypeNotSupported(String),

    #[error("Date time conversion failed")]
    DateTimeConversionError,

    #[error("Date conversion failed")]
    DateConversionError,

    #[error("Time conversion failed")]
    TimeConversionError,

    #[error("Duration conversion failed")]
    DurationConversionError,

    #[error("Schema has {0} fields, but batch has {1}")]
    SchemaMismatchError(usize, usize),

    #[error(transparent)]
    ArrowError(#[from] ArrowError),
}
