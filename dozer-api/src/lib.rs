use arc_swap::ArcSwap;
use dozer_cache::{
    cache::{CacheWriteOptions, RwCacheManager},
    dozer_log::{errors::SchemaError, home_dir::HomeDir, schemas::load_schema},
    errors::CacheError,
    CacheReader,
};
use dozer_types::{
    grpc_types::types::Operation,
    log::info,
    models::api_endpoint::{
        ApiEndpoint, OnDeleteResolutionTypes, OnInsertResolutionTypes, OnUpdateResolutionTypes,
        SecondaryIndexConfig,
    },
};
use futures_util::Future;
use std::{
    borrow::Cow,
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
        runtime: Arc<Runtime>,
        cancel: impl Future<Output = ()> + Unpin,
        operations_sender: Option<Sender<Operation>>,
        multi_pb: Option<MultiProgress>,
    ) -> Result<(Self, Option<impl FnOnce() -> Result<(), CacheError>>), ApiError> {
        let migration_path = if let Some(version) = endpoint.version {
            home_dir
                .find_migration_path(&endpoint.name, version)
                .ok_or(ApiError::MigrationNotFound(endpoint.name.clone(), version))?
        } else {
            home_dir
                .find_latest_migration_path(&endpoint.name)
                .map_err(|(path, error)| SchemaError::Filesystem(path, error))?
                .ok_or(ApiError::NoMigrationFound(endpoint.name.clone()))?
        };

        let (cache_reader, task) = if let Some(cache_reader) =
            open_cache_reader(cache_manager, &endpoint.name)?
        {
            (cache_reader, None)
        } else {
            let schema = load_schema(&migration_path.schema_path)?.schema;
            let secondary_index_config = get_secondary_index_config(&endpoint);
            let operations_sender = operations_sender.map(|sender| (endpoint.name.clone(), sender));
            let conflict_resolution = endpoint.conflict_resolution.unwrap_or_default();
            let write_options = CacheWriteOptions {
                insert_resolution: OnInsertResolutionTypes::from(conflict_resolution.on_insert),
                delete_resolution: OnDeleteResolutionTypes::from(conflict_resolution.on_delete),
                update_resolution: OnUpdateResolutionTypes::from(conflict_resolution.on_update),
                ..Default::default()
            };
            let (cache_name, task) = cache_builder::create_cache(
                cache_manager,
                schema,
                &secondary_index_config,
                runtime,
                cancel,
                &migration_path.log_path,
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
                descriptor_path: migration_path.descriptor_path,
                endpoint,
            },
            task,
        ))
    }

    pub fn open(
        cache_manager: &dyn RwCacheManager,
        descriptor_path: PathBuf,
        endpoint: ApiEndpoint,
    ) -> Result<Self, ApiError> {
        Ok(Self {
            cache_reader: ArcSwap::from_pointee(open_existing_cache_reader(
                cache_manager,
                &endpoint.name,
            )?),
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

fn get_secondary_index_config(api_endpoint: &ApiEndpoint) -> Cow<SecondaryIndexConfig> {
    if let Some(config) = api_endpoint
        .index
        .as_ref()
        .and_then(|index| index.secondary.as_ref())
    {
        Cow::Borrowed(config)
    } else {
        Cow::Owned(SecondaryIndexConfig::default())
    }
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
pub use async_trait;
use dozer_types::indicatif::MultiProgress;
use errors::ApiError;
pub use openapiv3;
pub use tokio;
use tokio::{runtime::Runtime, sync::broadcast::Sender};
pub use tonic;
pub use tracing_actix_web;

#[cfg(test)]
mod test_utils;
