use std::net::{AddrParseError, SocketAddr};
use std::path::PathBuf;

use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use actix_web::HttpResponse;
use dozer_cache::dozer_log::errors::ReaderBuilderError;
use dozer_tracing::Labels;
use dozer_types::errors::internal::BoxedError;
use dozer_types::errors::types::{CannotConvertF64ToJson, TypeError};
use dozer_types::thiserror::Error;
use dozer_types::{bincode, serde_json, thiserror, tonic};

use dozer_cache::errors::CacheError;
use handlebars::{RenderError, TemplateError};
use prost_reflect::{DescriptorError, Kind};

#[derive(Debug, Error)]
pub enum ApiInitError {
    #[error("Grpc error: {0}")]
    Grpc(#[from] GrpcError),
    #[error("Generation error: {0}")]
    Generation(#[from] GenerationError),
    #[error("Failed to create log reader")]
    ReaderBuilder(#[from] ReaderBuilderError),
    #[error("Failed to connect to app server {url}: {error}")]
    ConnectToAppServer {
        url: String,
        #[source]
        error: tonic::transport::Error,
    },
    #[error("Failed to get log metadata: {0}")]
    GetLogMetadata(#[from] tonic::Status),
    #[error("Query cache: {0}")]
    GetCacheCommitState(#[source] CacheError),
    #[error("Failed to parse endpoint schema: {0}")]
    ParseEndpointSchema(#[from] serde_json::Error),
    #[error("Failed to parse checkpoint: {0}")]
    ParseCheckpoint(#[from] bincode::Error),
    #[error("Failed to open or create cache: {0}")]
    OpenOrCreateCache(#[source] CacheError),
    #[error("Failed to find cache: {0}")]
    CacheNotFound(Labels),
    #[error("Failed to bind to address {0}: {1}")]
    FailedToBindToAddress(String, #[source] std::io::Error),
}

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Authentication error: {0}")]
    ApiAuthError(#[from] AuthError),
    #[error("Get by primary key is not supported when there is no primary key")]
    NoPrimaryKey,
    #[error("Get by primary key is not supported when it is composite: {0:?}")]
    MultiIndexFetch(String),
    #[error("Document not found")]
    NotFound(#[source] CacheError),
    #[error("Failed to count records")]
    CountFailed(#[source] CacheError),
    #[error("Failed to query cache: {0}")]
    QueryFailed(#[source] CacheError),
    #[error("Failed to get cache phase: {0}")]
    GetPhaseFailed(#[source] CacheError),
    #[error("Invalid primary key: {0}")]
    InvalidPrimaryKey(#[source] TypeError),
    #[error("Invalid access filter: {0}")]
    InvalidAccessFilter(#[source] serde_json::Error),
    #[error(transparent)]
    CannotConvertF64ToJson(#[from] CannotConvertF64ToJson),
}

#[derive(Error, Debug)]
pub enum GrpcError {
    #[error("Server reflection error: {0}")]
    ServerReflectionError(#[from] tonic_reflection::server::Error),
    #[error("Addr parse error: {0}: {1}")]
    AddrParse(String, #[source] AddrParseError),
    #[error("Failed to listen to address {0}: {1:?}")]
    Listen(SocketAddr, #[source] BoxedError),
}

impl From<ApiError> for dozer_types::tonic::Status {
    fn from(input: ApiError) -> Self {
        dozer_types::tonic::Status::new(dozer_types::tonic::Code::Unknown, input.to_string())
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
    #[error("Failed to create proto descriptor: {0}")]
    FailedToCreateProtoDescriptor(#[source] std::io::Error),
    #[error("Failed to read proto descriptor {0:?}: {1}")]
    FailedToReadProtoDescriptor(PathBuf, #[source] std::io::Error),
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
    #[error("JWT error: {0}")]
    JWT(#[from] jsonwebtoken::errors::Error),
}

impl actix_web::error::ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .body(self.to_string())
    }

    fn status_code(&self) -> StatusCode {
        match *self {
            ApiError::InvalidPrimaryKey(_) | ApiError::InvalidAccessFilter(_) => {
                StatusCode::BAD_REQUEST
            }
            ApiError::ApiAuthError(_) => StatusCode::UNAUTHORIZED,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::NoPrimaryKey | ApiError::MultiIndexFetch(_) => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            ApiError::QueryFailed(_)
            | ApiError::CountFailed(_)
            | ApiError::GetPhaseFailed(_)
            | ApiError::CannotConvertF64ToJson(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
