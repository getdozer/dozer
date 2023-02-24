use std::{ops::Deref, sync::Arc};

use arc_swap::ArcSwap;
use dozer_cache::{cache::CacheManager, CacheReader};
use dozer_types::models::api_endpoint::ApiEndpoint;
mod api_helper;

#[derive(Debug)]
pub struct RoCacheEndpoint {
    cache_reader: ArcSwap<CacheReader>,
    endpoint: ApiEndpoint,
}

impl RoCacheEndpoint {
    pub fn new(cache_manager: &dyn CacheManager, endpoint: ApiEndpoint) -> Result<Self, ApiError> {
        let cache_reader = open_cache_reader(cache_manager, &endpoint.name)?;
        Ok(Self {
            cache_reader: ArcSwap::from_pointee(cache_reader),
            endpoint,
        })
    }

    pub fn cache_reader(&self) -> impl Deref<Target = Arc<CacheReader>> + '_ {
        self.cache_reader.load()
    }

    pub fn endpoint(&self) -> &ApiEndpoint {
        &self.endpoint
    }

    pub fn redirect_cache(&self, cache_manager: &dyn CacheManager) -> Result<(), ApiError> {
        let cache_reader = open_cache_reader(cache_manager, &self.endpoint.name)?;
        self.cache_reader.store(Arc::new(cache_reader));
        Ok(())
    }
}

fn open_cache_reader(
    cache_manager: &dyn CacheManager,
    name: &str,
) -> Result<CacheReader, ApiError> {
    let cache = cache_manager
        .open_ro_cache(name)
        .map_err(|e| ApiError::OpenCache(e))?;
    let cache = cache.ok_or_else(|| ApiError::CacheNotFound(name.to_string()))?;
    Ok(CacheReader::new(cache))
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
use errors::ApiError;
pub use openapiv3;
pub use tokio;
pub use tonic;

#[cfg(test)]
mod test_utils;
