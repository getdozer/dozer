use dozer_cache::cache::LmdbCache;
use dozer_types::models::api_endpoint::ApiEndpoint;
use std::sync::Arc;
mod api_helper;

#[derive(Clone, Debug)]
pub struct CacheEndpoint {
    pub cache: Arc<LmdbCache>,
    pub endpoint: ApiEndpoint,
}

impl CacheEndpoint {
    pub fn new(cache: Arc<LmdbCache>, endpoint: ApiEndpoint) -> Result<Self, CacheError> {
        Ok(Self { cache, endpoint })
    }
}

#[derive(Clone)]
pub struct PipelineDetails {
    pub schema_name: String,
    pub cache_endpoint: CacheEndpoint,
}

// Exports
pub mod auth;
pub mod errors;
pub mod generator;
pub mod grpc;
pub mod rest;
// Re-exports
pub use actix_web;
use dozer_cache::errors::CacheError;
pub use openapiv3;
pub use tokio;
pub use tonic;

#[cfg(test)]
mod test_utils;
