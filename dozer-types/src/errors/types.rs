use super::internal::BoxedError;
use crate::{json_types::JsonValue, types::FieldType};
use serde_json::Number;
use std::num::ParseIntError;
use thiserror::Error;
use tokio_postgres::Error;

#[derive(Error, Debug)]
pub enum TypeError {
    #[error("Invalid field index: {0}")]
    InvalidFieldIndex(usize),
    #[error("Invalid field name: {0}")]
    InvalidFieldName(String),
    #[error("Invalid field type")]
    InvalidFieldType,
    #[error("Invalid field value: {value}, field type: {field_type}, nullable: {nullable}")]
    InvalidFieldValue {
        field_type: FieldType,
        nullable: bool,
        value: String,
    },
    #[error("Serialization failed: {0}")]
    SerializationError(#[source] SerializationError),
    #[error("Failed to parse the field: {0}")]
    DeserializationError(#[from] DeserializationError),
}

#[derive(Error, Debug)]
pub enum SerializationError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("bincode: {0}")]
    Bincode(#[from] bincode::error::EncodeError),
    #[error("custom: {0}")]
    Custom(#[from] BoxedError),
}

#[derive(Error, Debug)]
pub enum DeserializationError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("bincode: {0}")]
    Bincode(#[from] bincode::error::DecodeError),
    #[error("bson: {0}")]
    Msgpack(#[from] rmp_serde::decode::Error),
    #[error("json value {1:?} doesn't match field type {0}")]
    JsonType(FieldType, JsonValue),
    #[error("Empty input")]
    EmptyInput,
    #[error("Unrecognised field type : {0}")]
    UnrecognisedFieldType(u8),
    #[error("Bad data length")]
    BadDataLength,
    #[error("Bad data format: {0}")]
    BadDateFormat(#[from] chrono::ParseError),
    #[error("Ambiguous timestamp")]
    AmbiguousTimestamp,
    #[error("Invalid timestamp")]
    InvalidTimestamp,
    #[error("utf8: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("Failed to convert type due to json numbers being out of the f64 range: {0}")]
    F64TypeConversionError(Number),
    #[error("Unknown SSL mode: {0}")]
    UnknownSslMode(String),
    #[error("Unable to Parse Postgres configuration: {0}")]
    UnableToParseConnectionUrl(ParseIntError),
    #[error("Invalid connection url for Postgres configuration: {0}")]
    InvalidConnectionUrl(Error),
    #[error("{0} is missing in Postgres configuration")]
    MissingFieldInPostgresConfig(String),
    #[error("{0} is mismatching in Postgres configuration")]
    MismatchingFieldInPostgresConfig(String),
}

#[derive(Debug, Error)]
#[error("Cannot convert f64 to json: {0}")]
pub struct CannotConvertF64ToJson(pub f64);
