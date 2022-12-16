mod auth;
use dozer_cache::cache::LmdbCache;
use dozer_types::models::api_endpoint::ApiEndpoint;
use std::sync::Arc;
mod api_helper;
mod generator;

#[derive(Clone)]
pub struct CacheEndpoint {
    pub cache: Arc<LmdbCache>,
    pub endpoint: ApiEndpoint,
}

#[derive(Clone)]
pub struct PipelineDetails {
    pub schema_name: String,
    pub cache_endpoint: CacheEndpoint,
}

// Exports
pub mod errors;
pub mod grpc;
pub mod rest;
// Re-exports
pub use actix_web;
pub use tokio;

#[cfg(test)]
mod test_utils;
