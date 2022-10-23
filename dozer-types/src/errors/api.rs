// #![allow(clippy::enum_variant_names)]

// use thiserror;
// use thiserror::Error;

// use super::cache::CacheError;
// use super::internal::BoxedError;
// use super::types::TypeError;

// #[derive(Error, Debug)]
// pub enum ApiError {
//     #[error(transparent)]
//     ApiValidationError(#[from] ApiValidationError),
//     #[error(transparent)]
//     ApiAuthError(#[from] ApiAuthError),
//     #[error("Failed to generate openapi documentation")]
//     ApiGenerationError(#[source] ApiGenerationError),
//     #[error("Cannot find schema by name")]
//     SchemaNotFound(#[source] CacheError),
//     #[error("Document not found")]
//     NotFound(#[source] CacheError),
//     #[error(transparent)]
//     InternalError(#[from] BoxedError),
//     #[error("pipeline_details not initialized")]
//     PipelineNotInitialized,
//     #[error(transparent)]
//     TypeError(#[from] TypeError),
// }

// impl ApiError {
//     pub fn map_serialization_error(e: serde_json::Error) -> ApiError {
//         ApiError::TypeError(TypeError::SerializationError(
//             super::types::SerializationError::Json(e),
//         ))
//     }
//     pub fn map_deserialization_error(e: serde_json::Error) -> ApiError {
//         ApiError::TypeError(TypeError::DeserializationError(
//             super::types::DeserializationError::Json(e),
//         ))
//     }
// }

// #[derive(Error, Debug)]
// pub enum ApiValidationError {}

// #[derive(Error, Debug)]
// pub enum ApiGenerationError {
//     #[error("Cannot create temporary file")]
//     TmpFile(#[source] std::io::Error),
//     #[error("Cannot serialize to file")]
//     SerializationError(#[source] serde_json::Error),
// }

// #[derive(Error, Debug)]
// pub enum ApiAuthError {
//     #[error("Invalid token provided")]
//     InvalidToken,
//     #[error("Issuer is invalid")]
//     InvalidIssuer,
//     #[error(transparent)]
//     InternalError(#[from] BoxedError),
// }
