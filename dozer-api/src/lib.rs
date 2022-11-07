mod api_helper;
mod auth;
use dozer_cache::cache::LmdbCache;
use dozer_types::models::api_endpoint::ApiEndpoint;
use std::sync::Arc;
mod grpc;

#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod tests;

#[derive(Clone)]
pub struct CacheEndpoint {
    pub cache: Arc<LmdbCache>,
    pub endpoint: ApiEndpoint,
}

// Exports
pub mod api_generator;
pub mod api_server;
pub mod errors;
pub mod generator;
pub mod grpc_server;

// Re-exports
pub use actix_web;
