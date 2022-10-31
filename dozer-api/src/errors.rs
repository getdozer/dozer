#![allow(clippy::enum_variant_names)]

use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use actix_web::HttpResponse;
use dozer_types::thiserror::Error;
use dozer_types::{serde_json, thiserror};

use dozer_types::errors::cache::{CacheError, QueryValidationError};
use dozer_types::errors::internal::BoxedError;
use dozer_types::errors::types::TypeError;
use handlebars::{RenderError, TemplateError};

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Invalid query provided")]
    InvalidQuery(#[source] QueryValidationError),
    #[error(transparent)]
    ApiAuthError(#[from] AuthError),
    #[error("Failed to generate openapi documentation")]
    ApiGenerationError(#[source] GenerationError),
    #[error("Cannot find schema by name")]
    SchemaNotFound(#[source] CacheError),
    #[error("Document not found")]
    NotFound(#[source] CacheError),
    #[error(transparent)]
    InternalError(#[from] BoxedError),
    #[error(transparent)]
    InitError(#[from] InitError),
    #[error(transparent)]
    TypeError(#[from] TypeError),
    #[error("Schema Identifier is not present")]
    SchemaIdentifierNotFound,
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
pub enum GRPCError {
    #[error("Internal GRPC server error: {0}")]
    InternalError(String),
    #[error("Cannot get Proto descriptor: {0}")]
    ProtoDescriptorError(String),
    #[error(transparent)]
    SerizalizeError(#[from] serde_json::Error),
    #[error("Missing primary key to query by id: {0}")]
    MissingPrimaryKeyToQueryById(String),
    #[error(transparent)]
    GenerationError(#[from] GenerationError),
    #[error(transparent)]
    SchemaNotFound(#[from] CacheError),
    #[error(transparent)]
    ServerReflectionError(#[from] tonic_reflection::server::Error),
    #[error("Unable to decode query expression: {0}")]
    UnableToDecodeQueryExpression(String),
}
impl From<GRPCError> for tonic::Status {
    fn from(input: GRPCError) -> Self {
        tonic::Status::new(tonic::Code::Internal, input.to_string())
    }
}

impl From<ApiError> for tonic::Status {
    fn from(input: ApiError) -> Self {
        tonic::Status::new(tonic::Code::Unknown, input.to_string())
    }
}

#[derive(Error, Debug)]
pub enum InitError {
    #[error("pipeline_details not initialized")]
    PipelineNotInitialized,
    #[error("api_security not initialized")]
    SecurityNotInitialized,
}

#[derive(Error, Debug)]
pub enum GenerationError {
    #[error("Cannot create temporary file")]
    TmpFile(#[source] std::io::Error),
    #[error("Cannot serialize to file")]
    SerializationError(#[source] serde_json::Error),
    #[error("Cannot register template")]
    TemplateError(#[source] TemplateError),
    #[error("directory path does not exist")]
    DirPathNotExist,
    #[error("Read file buffer error")]
    ReadFileBuffer(#[source] std::io::Error),
    #[error("Cannot open file")]
    FileCannotOpen(#[source] std::io::Error),
    #[error("Cannot render with handlebars template")]
    RenderError(#[source] RenderError),
    #[error("DozerType to Proto type not supported: {0}")]
    DozerToProtoTypeNotSupported(String),
    #[error("Missing primary key to query by id: {0}")]
    MissingPrimaryKeyToQueryById(String),
    #[error("Unable to create proto descriptor: {0}")]
    CannotCreateProtoDescriptor(String),
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
            ApiError::InvalidQuery(_) => StatusCode::BAD_REQUEST,
            ApiError::ApiAuthError(_) => StatusCode::UNAUTHORIZED,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::InternalError(_)
            | ApiError::InitError(_)
            | ApiError::SchemaIdentifierNotFound => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::ApiGenerationError(_)
            | ApiError::SchemaNotFound(_)
            | ApiError::TypeError(_) => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }
}
