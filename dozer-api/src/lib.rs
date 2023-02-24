use dozer_cache::{cache::RoCache, CacheReader};
use dozer_types::models::api_endpoint::ApiEndpoint;
mod api_helper;

#[derive(Debug)]
pub struct RoCacheEndpoint {
    pub cache_reader: CacheReader,
    pub endpoint: ApiEndpoint,
}

impl RoCacheEndpoint {
    pub fn new(cache: Box<dyn RoCache>, endpoint: ApiEndpoint) -> Self {
        Self {
            cache_reader: CacheReader::new(cache),
            endpoint,
        }
    }
}

// Exports
pub mod auth;
pub mod errors;
pub mod generator;
pub mod grpc;
pub mod rest;
// Re-exports
pub use actix_web;
pub use async_trait;
pub use openapiv3;
pub use tokio;
pub use tonic;

#[cfg(test)]
mod test_utils;
