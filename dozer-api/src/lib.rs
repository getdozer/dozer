use arc_swap::ArcSwap;
use dozer_cache::{
    cache::{CacheWriteOptions, RwCacheManager},
    errors::CacheError,
    CacheReader,
};
use dozer_types::{
    grpc_types::types::Operation,
    indicatif::MultiProgress,
    log::info,
    models::api_endpoint::{
        ApiEndpoint, OnDeleteResolutionTypes, OnInsertResolutionTypes, OnUpdateResolutionTypes,
    },
    types::Schema,
};
use std::{ops::Deref, path::Path, sync::Arc};

mod api_helper;

#[derive(Debug)]
pub struct CacheEndpoint {
    cache_reader: ArcSwap<CacheReader>,
    endpoint: ApiEndpoint,
}

impl CacheEndpoint {
    pub async fn new(
        cache_manager: &dyn RwCacheManager,
        schema: Schema,
        endpoint: ApiEndpoint,
        runtime: Arc<Runtime>,
        log_path: &Path,
        operations_sender: Option<Sender<Operation>>,
        multi_pb: Option<MultiProgress>,
    ) -> Result<(Self, Option<impl FnOnce() -> Result<(), CacheError>>), ApiError> {
        let (cache_reader, task) = if let Some(cache_reader) =
            open_cache_reader(cache_manager, &endpoint.name)?
        {
            (cache_reader, None)
        } else {
            let operations_sender = operations_sender.map(|sender| (endpoint.name.clone(), sender));
            let conflict_resolution = endpoint.conflict_resolution.unwrap_or_default();
            let write_options = CacheWriteOptions {
                insert_resolution: OnInsertResolutionTypes::from(conflict_resolution.on_insert),
                delete_resolution: OnDeleteResolutionTypes::from(conflict_resolution.on_delete),
                update_resolution: OnUpdateResolutionTypes::from(conflict_resolution.on_update),
            };
            let (cache_name, task) = cache_builder::create_cache(
                cache_manager,
                schema,
                runtime,
                log_path,
                write_options,
                operations_sender,
                multi_pb,
            )
            .await
            .map_err(ApiError::CreateCache)?;
            // TODO: We intentionally don't create alias endpoint.name -> cache_name here.
            (
                open_cache_reader(cache_manager, &cache_name)?.expect("We just created the cache"),
                Some(task),
            )
        };
        Ok((
            Self {
                cache_reader: ArcSwap::from_pointee(cache_reader),
                endpoint,
            },
            task,
        ))
    }

    pub fn open(
        cache_manager: &dyn RwCacheManager,
        endpoint: ApiEndpoint,
    ) -> Result<Self, ApiError> {
        Ok(Self {
            cache_reader: ArcSwap::from_pointee(open_existing_cache_reader(
                cache_manager,
                &endpoint.name,
            )?),
            endpoint,
        })
    }

    pub fn cache_reader(&self) -> impl Deref<Target = Arc<CacheReader>> + '_ {
        self.cache_reader.load()
    }

    pub fn endpoint(&self) -> &ApiEndpoint {
        &self.endpoint
    }

    pub fn redirect_cache(&self, cache_manager: &dyn RwCacheManager) -> Result<(), ApiError> {
        self.cache_reader.store(Arc::new(open_existing_cache_reader(
            cache_manager,
            &self.endpoint.name,
        )?));
        Ok(())
    }
}

fn open_cache_reader(
    cache_manager: &dyn RwCacheManager,
    name: &str,
) -> Result<Option<CacheReader>, ApiError> {
    let cache = cache_manager
        .open_ro_cache(name)
        .map_err(ApiError::OpenCache)?;
    Ok(cache.map(|cache| {
        info!("[api] Serving {} using cache {}", name, cache.name());
        CacheReader::new(cache)
    }))
}

fn open_existing_cache_reader(
    cache_manager: &dyn RwCacheManager,
    name: &str,
) -> Result<CacheReader, ApiError> {
    open_cache_reader(cache_manager, name)?.ok_or_else(|| ApiError::CacheNotFound(name.to_string()))
}

// Exports
pub mod auth;
mod cache_builder;
pub use cache_builder::LogReader;
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
use tokio::{runtime::Runtime, sync::broadcast::Sender};
pub use tonic;

#[cfg(test)]
mod test_utils;
