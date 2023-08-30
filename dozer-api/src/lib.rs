use arc_swap::ArcSwap;
use cache_builder::open_or_create_cache;
use dozer_cache::{
    cache::{CacheWriteOptions, RwCacheManager},
    dozer_log::reader::{LogReaderBuilder, LogReaderOptions},
    errors::CacheError,
    CacheReader,
};
use dozer_tracing::{Labels, LabelsAndProgress};
use dozer_types::{
    grpc_types::types::Operation,
    models::api_endpoint::{
        default_log_reader_batch_size, default_log_reader_buffer_size,
        default_log_reader_timeout_in_millis, ApiEndpoint,
    },
};
use futures_util::Future;
use std::{ops::Deref, sync::Arc};

pub use tonic_reflection;
pub use tonic_web;
pub use tower_http;
mod api_helper;

#[derive(Debug)]
pub struct CacheEndpoint {
    cache_reader: ArcSwap<CacheReader>,
    descriptor: Vec<u8>,
    endpoint: ApiEndpoint,
}

const ENDPOINT_LABEL: &str = "endpoint";
const BUILD_LABEL: &str = "build";

impl CacheEndpoint {
    pub async fn new(
        app_server_addr: String,
        cache_manager: &dyn RwCacheManager,
        endpoint: ApiEndpoint,
        cancel: impl Future<Output = ()> + Unpin + Send + 'static,
        operations_sender: Option<Sender<Operation>>,
        labels: LabelsAndProgress,
    ) -> Result<(Self, JoinHandle<Result<(), CacheError>>), ApiInitError> {
        // Create log reader builder.
        let log_reader_builder =
            LogReaderBuilder::new(app_server_addr, get_log_reader_options(&endpoint)).await?;
        let descriptor = log_reader_builder.descriptor.clone();

        // Open or create cache.
        let mut cache_labels =
            cache_labels(endpoint.name.clone(), log_reader_builder.build_name.clone());
        cache_labels.extend(labels.labels().clone());
        let schema = log_reader_builder.schema.clone();
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
        .map_err(ApiInitError::OpenOrCreateCache)?;

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
                    log_reader_builder,
                    operations_sender,
                    labels,
                )
                .await
            })
        };

        Ok((
            Self {
                cache_reader: ArcSwap::from_pointee(cache_reader),
                descriptor,
                endpoint,
            },
            handle,
        ))
    }

    pub fn open(
        cache_manager: &dyn RwCacheManager,
        descriptor: Vec<u8>,
        endpoint: ApiEndpoint,
    ) -> Result<Self, ApiInitError> {
        let mut labels = Labels::new();
        labels.push(endpoint.name.clone(), endpoint.name.clone());
        Ok(Self {
            cache_reader: ArcSwap::from_pointee(open_existing_cache_reader(cache_manager, labels)?),
            descriptor,
            endpoint,
        })
    }

    pub fn cache_reader(&self) -> impl Deref<Target = Arc<CacheReader>> + '_ {
        self.cache_reader.load()
    }

    pub fn descriptor(&self) -> &[u8] {
        &self.descriptor
    }

    pub fn endpoint(&self) -> &ApiEndpoint {
        &self.endpoint
    }
}

pub fn cache_labels(endpoint: String, build: String) -> Labels {
    let mut labels = Labels::new();
    labels.push(ENDPOINT_LABEL, endpoint);
    labels.push(BUILD_LABEL, build);
    labels
}

fn open_cache_reader(
    cache_manager: &dyn RwCacheManager,
    labels: Labels,
) -> Result<Option<CacheReader>, ApiInitError> {
    let cache = cache_manager
        .open_ro_cache(labels)
        .map_err(ApiInitError::OpenOrCreateCache)?;
    Ok(cache.map(CacheReader::new))
}

fn open_existing_cache_reader(
    cache_manager: &dyn RwCacheManager,
    labels: Labels,
) -> Result<CacheReader, ApiInitError> {
    open_cache_reader(cache_manager, labels.clone())?
        .ok_or_else(|| ApiInitError::CacheNotFound(labels))
}

fn get_log_reader_options(endpoint: &ApiEndpoint) -> LogReaderOptions {
    LogReaderOptions {
        endpoint: endpoint.name.clone(),
        batch_size: endpoint
            .log_reader_options
            .as_ref()
            .and_then(|options| options.batch_size)
            .unwrap_or_else(default_log_reader_batch_size),
        timeout_in_millis: endpoint
            .log_reader_options
            .as_ref()
            .and_then(|options| options.timeout_in_millis)
            .unwrap_or_else(default_log_reader_timeout_in_millis),
        buffer_size: endpoint
            .log_reader_options
            .as_ref()
            .and_then(|options| options.buffer_size)
            .unwrap_or_else(default_log_reader_buffer_size),
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
pub use actix_web_httpauth;
pub use api_helper::API_LATENCY_HISTOGRAM_NAME;
pub use api_helper::API_REQUEST_COUNTER_NAME;
pub use async_trait;
use errors::ApiInitError;
pub use openapiv3;
pub use tokio;
use tokio::{sync::broadcast::Sender, task::JoinHandle};
pub use tonic;
pub use tracing_actix_web;
#[cfg(test)]
mod test_utils;
