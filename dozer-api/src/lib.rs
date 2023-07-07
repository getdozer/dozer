use arc_swap::ArcSwap;
use cache_builder::open_or_create_cache;
use dozer_cache::{
    cache::{CacheWriteOptions, RwCacheManager},
    dozer_log::{errors::SchemaError, home_dir::HomeDir, schemas::load_schema},
    errors::CacheError,
    CacheReader,
};
use dozer_types::{
    grpc_types::types::Operation, labels::Labels, models::api_endpoint::ApiEndpoint,
};
use futures_util::Future;
use std::{
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

mod api_helper;

#[derive(Debug)]
pub struct CacheEndpoint {
    cache_reader: ArcSwap<CacheReader>,
    descriptor_path: PathBuf,
    endpoint: ApiEndpoint,
}

impl CacheEndpoint {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        home_dir: &HomeDir,
        cache_manager: &dyn RwCacheManager,
        endpoint: ApiEndpoint,
        cancel: impl Future<Output = ()> + Unpin + Send + 'static,
        operations_sender: Option<Sender<Operation>>,
        multi_pb: Option<MultiProgress>,
    ) -> Result<(Self, JoinHandle<Result<(), CacheError>>), ApiError> {
        // Find migration.
        let migration_path = if let Some(version) = endpoint.version {
            home_dir
                .find_migration_path(&endpoint.name, version)
                .ok_or(ApiError::MigrationNotFound(endpoint.name.clone(), version))?
        } else {
            home_dir
                .find_latest_migration_path(&endpoint.name)
                .map_err(|(path, error)| SchemaError::Filesystem(path.into(), error))?
                .ok_or(ApiError::NoMigrationFound(endpoint.name.clone()))?
        };

        // Open or create cache.
        let mut cache_labels = Labels::new();
        cache_labels.push("endpoint", endpoint.name.clone());
        cache_labels.push("migration", migration_path.id.name().to_string());
        let schema = load_schema(&migration_path.schema_path)?;
        let conflict_resolution = endpoint.conflict_resolution.unwrap_or_default();
        let write_options = CacheWriteOptions {
            insert_resolution: conflict_resolution.on_insert.unwrap_or_default(),
            delete_resolution: conflict_resolution.on_delete.unwrap_or_default(),
            update_resolution: conflict_resolution.on_update.unwrap_or_default(),
            ..Default::default()
        };
        let cache = open_or_create_cache(
            cache_manager,
            cache_labels.clone(),
            (schema.schema, schema.secondary_indexes),
            &schema.connections,
            write_options,
        )
        .map_err(ApiError::OpenOrCreateCache)?;

        // Open cache reader.
        let cache_reader =
            open_cache_reader(cache_manager, cache_labels)?.expect("We just created the cache");

        // Start cache builder.
        let handle = {
            let operations_sender = operations_sender.map(|sender| (endpoint.name.clone(), sender));
            tokio::spawn(async move {
                cache_builder::build_cache(
                    cache,
                    cancel,
                    &migration_path.log_path,
                    operations_sender,
                    multi_pb,
                )
                .await
            })
        };

        Ok((
            Self {
                cache_reader: ArcSwap::from_pointee(cache_reader),
                descriptor_path: migration_path.descriptor_path.into(),
                endpoint,
            },
            handle,
        ))
    }

    pub fn open(
        cache_manager: &dyn RwCacheManager,
        descriptor_path: PathBuf,
        endpoint: ApiEndpoint,
    ) -> Result<Self, ApiError> {
        let mut labels = Labels::new();
        labels.push(endpoint.name.clone(), endpoint.name.clone());
        Ok(Self {
            cache_reader: ArcSwap::from_pointee(open_existing_cache_reader(cache_manager, labels)?),
            descriptor_path,
            endpoint,
        })
    }

    pub fn cache_reader(&self) -> impl Deref<Target = Arc<CacheReader>> + '_ {
        self.cache_reader.load()
    }

    pub fn descriptor_path(&self) -> &Path {
        &self.descriptor_path
    }

    pub fn endpoint(&self) -> &ApiEndpoint {
        &self.endpoint
    }
}

fn open_cache_reader(
    cache_manager: &dyn RwCacheManager,
    labels: Labels,
) -> Result<Option<CacheReader>, ApiError> {
    let cache = cache_manager
        .open_ro_cache(labels)
        .map_err(ApiError::OpenOrCreateCache)?;
    Ok(cache.map(CacheReader::new))
}

fn open_existing_cache_reader(
    cache_manager: &dyn RwCacheManager,
    labels: Labels,
) -> Result<CacheReader, ApiError> {
    open_cache_reader(cache_manager, labels.clone())?.ok_or_else(|| ApiError::CacheNotFound(labels))
}

// Exports
pub mod auth;
mod cache_builder;
pub mod errors;
pub mod generator;
pub mod grpc;
pub mod rest;
// Re-exports
pub use actix_cors;
pub use actix_web;
pub use actix_web_httpauth;
pub use api_helper::API_LATENCY_HISTOGRAM_NAME;
pub use api_helper::API_REQUEST_COUNTER_NAME;
pub use async_trait;
use dozer_types::indicatif::MultiProgress;
use errors::ApiError;
pub use openapiv3;
pub use tokio;
use tokio::{sync::broadcast::Sender, task::JoinHandle};
pub use tonic;
pub use tracing_actix_web;
#[cfg(test)]
mod test_utils;
