use std::str::Utf8Error;

use base64::DecodeError;
use dozer_ingestion_connector::dozer_types::{
    rust_decimal, serde_json,
    thiserror::{self, Error},
};
use schema_registry_converter::error::SRCError;

pub mod connector;
pub mod debezium;
pub mod no_schema_registry_basic;
pub mod schema_registry_basic;
pub mod stream_consumer;
pub mod stream_consumer_basic;
mod stream_consumer_helper;
#[cfg(any(test, feature = "debezium_bench"))]
pub mod test_utils;

#[derive(Error, Debug)]
pub enum KafkaError {
    #[error(transparent)]
    KafkaSchemaError(#[from] KafkaSchemaError),

    #[error("Connection error. Error: {0}")]
    KafkaConnectionError(#[from] rdkafka::error::KafkaError),

    #[error("JSON decode error. Error: {0}")]
    JsonDecodeError(#[source] serde_json::Error),

    #[error("Bytes convert error")]
    BytesConvertError(#[source] Utf8Error),

    #[error(transparent)]
    KafkaStreamError(#[from] KafkaStreamError),

    #[error("Schema registry fetch failed. Error: {0}")]
    SchemaRegistryFetchError(#[source] SRCError),

    #[error("Topic not defined")]
    TopicNotDefined,
}

#[derive(Error, Debug)]
pub enum KafkaStreamError {
    #[error("Consume commit error")]
    ConsumeCommitError(#[source] rdkafka::error::KafkaError),

    #[error("Message consume error")]
    MessageConsumeError(#[source] rdkafka::error::KafkaError),

    #[error("Polling error")]
    PollingError(#[source] rdkafka::error::KafkaError),
}

#[derive(Error, Debug, PartialEq)]
pub enum KafkaSchemaError {
    #[error("Schema definition not found")]
    SchemaDefinitionNotFound,

    #[error("Unsupported \"{0}\" type")]
    TypeNotSupported(String),

    #[error("Field \"{0}\" not found")]
    FieldNotFound(String),

    #[error("Binary decode error")]
    BinaryDecodeError(#[source] DecodeError),

    #[error("Scale not found")]
    ScaleNotFound,

    #[error("Scale is invalid")]
    ScaleIsInvalid,

    #[error("Decimal convert error")]
    DecimalConvertError(#[source] rust_decimal::Error),

    #[error("Invalid date")]
    InvalidDateError,

    #[error("Invalid json: {0}")]
    InvalidJsonError(String),

    // #[error("Invalid time")]
    // InvalidTimeError,
    #[error("Invalid timestamp")]
    InvalidTimestampError,
}

#[cfg(test)]
mod tests;
