#![allow(clippy::enum_variant_names)]
use std::path::PathBuf;

use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use actix_web::HttpResponse;
use dozer_types::thiserror::Error;
use dozer_types::{serde_json, thiserror};

use dozer_cache::errors::CacheError;
use dozer_types::errors::internal::BoxedError;
use dozer_types::errors::types::TypeError;
use handlebars::{RenderError, TemplateError};
use prost_reflect::{DescriptorError, Kind};

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Authentication error: {0}")]
    ApiAuthError(#[from] AuthError),
    #[error("Failed to open cache: {0}")]
    OpenCache(#[source] CacheError),
    #[error("Failed to open cache: {0}")]
    CacheNotFound(String),
    #[error("Cannot find schema by name")]
    SchemaNotFound(#[source] CacheError),
    #[error("Get by primary key is not supported when there is no primary key")]
    NoPrimaryKey,
    #[error("Get by primary key is not supported when it is composite: {0:?}")]
    MultiIndexFetch(String),
    #[error("Document not found")]
    NotFound(#[source] CacheError),
    #[error("Failed to count records")]
    CountFailed(#[source] CacheError),
    #[error("Failed to query cache")]
    QueryFailed(#[source] CacheError),
    #[error("Internal error: {0}")]
    InternalError(#[from] BoxedError),
    #[error("Type error: {0}")]
    TypeError(#[from] TypeError),
    #[error("Failed to bind to address {0}: {1}")]
    FailedToBindToAddress(String, #[source] std::io::Error),
}

impl ApiError {
    pub fn map_serialization_error(e: serde_json::Error) -> ApiError {
        ApiError::TypeError(TypeError::SerializationError(
            dozer_types::errors::types::SerializationError::Json(e),
        ))
    }
    pub fn map_deserialization_error(e: serde_json::Error) -> ApiError {
        ApiError::TypeError(TypeError::DeserializationError(
            dozer_types::errors::types::DeserializationError::Json(e),
        ))
    }
}

#[derive(Error, Debug)]
pub enum GrpcError {
    #[error("Internal gRPC server error: {0}")]
    InternalError(#[from] BoxedError),
    #[error("Cannot send to broadcast channel")]
    CannotSendToBroadcastChannel,
    #[error("Generation error: {0}")]
    GenerationError(#[from] GenerationError),
    #[error("Schema not found: {0}")]
    SchemaNotFound(#[from] CacheError),
    #[error("Server reflection error: {0}")]
    ServerReflectionError(#[from] tonic_reflection::server::Error),
    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
}
impl From<GrpcError> for tonic::Status {
    fn from(input: GrpcError) -> Self {
        tonic::Status::new(tonic::Code::Internal, input.to_string())
    }
}

impl From<ApiError> for tonic::Status {
    fn from(input: ApiError) -> Self {
        tonic::Status::new(tonic::Code::Unknown, input.to_string())
    }
}

#[derive(Error, Debug)]
pub enum GenerationError {
    // clippy says `TemplateError` is 136 bytes on stack and much larger than other variants, so we box it.
    #[error("Handlebars template error: {0}")]
    HandlebarsTemplate(#[source] Box<TemplateError>),
    #[error("Handlebars render error: {0}")]
    HandlebarsRender(#[from] RenderError),
    #[error("Failed to write to file {0:?}: {1}")]
    FailedToWriteToFile(PathBuf, #[source] std::io::Error),
    #[error("directory path {0:?} does not exist")]
    DirPathNotExist(PathBuf),
    #[error("DozerType to Proto type not supported: {0}")]
    DozerToProtoTypeNotSupported(String),
    #[error("Failed to create proto descriptor: {0}")]
    FailedToCreateProtoDescriptor(#[source] std::io::Error),
    #[error("Failed to read proto descriptor: {0}")]
    FailedToReadProtoDescriptor(#[source] std::io::Error),
    #[error("Failed to decode proto descriptor: {0}")]
    FailedToDecodeProtoDescriptor(#[source] DescriptorError),
    #[error("Service not found: {0}")]
    ServiceNotFound(String),
    #[error("Field not found: {field_name} in message: {message_name}")]
    FieldNotFound {
        message_name: String,
        field_name: String,
    },
    #[error("Expected message field: {filed_name}, but found: {actual:?}")]
    ExpectedMessageField { filed_name: String, actual: Kind },
    #[error("Unexpected method {0}")]
    UnexpectedMethod(String),
    #[error("Missing count method for: {0}")]
    MissingCountMethod(String),
    #[error("Missing query method for: {0}")]
    MissingQueryMethod(String),
}

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("Cannot access this route.")]
    Unauthorized,
    #[error("Invalid token provided")]
    InvalidToken,
    #[error("Issuer is invalid")]
    InvalidIssuer,
    #[error("Internal error: {0}")]
    InternalError(#[from] BoxedError),
}

impl actix_web::error::ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .body(self.to_string())
    }

    fn status_code(&self) -> StatusCode {
        match *self {
            ApiError::TypeError(_) => StatusCode::BAD_REQUEST,
            ApiError::ApiAuthError(_) => StatusCode::UNAUTHORIZED,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::SchemaNotFound(_) | ApiError::NoPrimaryKey | ApiError::MultiIndexFetch(_) => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            ApiError::InternalError(_)
            | ApiError::OpenCache(_)
            | ApiError::CacheNotFound(_)
            | ApiError::QueryFailed(_)
            | ApiError::CountFailed(_)
            | ApiError::FailedToBindToAddress(_, _) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
