pub mod connector;
mod ingest;

mod adapter;
use std::net::AddrParseError;

pub use adapter::{ArrowAdapter, DefaultAdapter, GrpcIngestMessage, GrpcIngestor, IngestAdapter};
use dozer_ingestion_connector::dozer_types::{
    arrow::error::ArrowError,
    arrow_types::errors::FromArrowError,
    grpc_types, serde_json,
    thiserror::{self, Error},
    tonic::transport,
    types::FieldType,
};
use dozer_ingestion_connector::schema_parser::SchemaParserError;

#[cfg(test)]
mod tests;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Schema parser error: {0}")]
    CannotReadFile(#[from] SchemaParserError),
    #[error("serde json error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("from arrow error: {0}")]
    FromArrow(#[from] FromArrowError),
    #[error("arrow error: {0}")]
    Arrow(#[from] ArrowError),
    #[error("cannot parse address: {0}")]
    AddrParse(#[from] AddrParseError),
    #[error("tonic transport error: {0}")]
    TonicTransport(#[from] transport::Error),
    #[error("default adapter cannot handle arrow ingest message")]
    CannotHandleArrowMessage,
    #[error("arrow adapter cannot handle default ingest message")]
    CannotHandleDefaultMessage,
    #[error("schema not found: {0}")]
    SchemaNotFound(String),
    #[error("record is not properly formed. Length of values {values_count} does not match schema: {schema_fields_count}")]
    NumFieldsMismatch {
        values_count: usize,
        schema_fields_count: usize,
    },
    #[error("data is not valid at index: {index}, Type: {value:?}, Expected Type: {field_type}")]
    FieldTypeMismatch {
        index: usize,
        value: grpc_types::types::value::Value,
        field_type: FieldType,
    },
}
