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
use prost_reflect::{DescriptorError, Kind};

#[derive(Error, Debug)]
pub enum ApiError {
    #[error(transparent)]
    ApiAuthError(#[from] AuthError),
    #[error("Failed to generate openapi documentation")]
    ApiGenerationError(#[source] GenerationError),
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
    #[error(transparent)]
    InternalError(#[from] BoxedError),
    #[error(transparent)]
    TypeError(#[from] TypeError),
    #[error(transparent)]
    PortAlreadyInUse(#[from] std::io::Error),
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
    #[error(transparent)]
    SerizalizeError(#[from] serde_json::Error),
    #[error("Missing primary key to query by id: {0}")]
    MissingPrimaryKeyToQueryById(String),
    #[error(transparent)]
    GenerationError(#[from] GenerationError),
    #[error(transparent)]
    SchemaNotFound(#[from] CacheError),
    #[error("{1}: Schema for endpoint: {0} not found")]
    SchemaNotInitialized(String, #[source] CacheError),
    #[error(transparent)]
    ServerReflectionError(#[from] tonic_reflection::server::Error),
    #[error("Unable to decode query expression: {0}")]
    UnableToDecodeQueryExpression(String),
    #[error("{0}")]
    TransportErrorDetail(String),
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
    #[error(transparent)]
    InternalError(#[from] BoxedError),
    #[error("directory path {0:?} does not exist")]
    DirPathNotExist(PathBuf),
    #[error("DozerType to Proto type not supported: {0}")]
    DozerToProtoTypeNotSupported(String),
    #[error("Missing primary key to query by id: {0}")]
    MissingPrimaryKeyToQueryById(String),
    #[error("Cannot read proto descriptor: {0}")]
    ProtoDescriptorError(#[source] DescriptorError),
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
    #[error(transparent)]
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
            ApiError::ApiGenerationError(_)
            | ApiError::SchemaNotFound(_)
            | ApiError::NoPrimaryKey
            | ApiError::MultiIndexFetch(_) => StatusCode::UNPROCESSABLE_ENTITY,
            ApiError::InternalError(_)
            | ApiError::OpenCache(_)
            | ApiError::CacheNotFound(_)
            | ApiError::QueryFailed(_)
            | ApiError::CountFailed(_)
            | ApiError::PortAlreadyInUse(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
