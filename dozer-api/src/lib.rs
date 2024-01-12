use arc_swap::ArcSwap;
use cache_builder::CacheBuilder;
use dozer_cache::{cache::RwCacheManager, errors::CacheError, CacheReader};
use dozer_tracing::{Labels, LabelsAndProgress};
use dozer_types::{grpc_types::types::Operation, models::endpoint::ApiEndpoint};
use futures_util::Future;
use generator::protoc::generate_all;
use std::{ops::Deref, sync::Arc};
use tempdir::TempDir;

pub use tonic_reflection;
pub use tonic_web;
pub use tower_http;
mod api_helper;
pub mod sql;
pub use api_helper::get_api_security;

#[derive(Debug)]
pub struct CacheEndpoint {
    cache_reader: Arc<ArcSwap<CacheReader>>,
    descriptor: Vec<u8>,
    table_name: String,
    endpoint: ApiEndpoint,
}

const ENDPOINT_LABEL: &str = "endpoint";

impl CacheEndpoint {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        runtime: Arc<Runtime>,
        app_server_url: String,
        table_name: String,
        cache_manager: Arc<dyn RwCacheManager>,
        endpoint: ApiEndpoint,
        cancel: impl Future<Output = ()> + Unpin + Send + 'static,
        operations_sender: Option<Sender<Operation>>,
        labels: LabelsAndProgress,
    ) -> Result<(Self, JoinHandle<Result<(), CacheError>>), ApiInitError> {
        // Create cache builder.
        let (cache_builder, endpoint_schema) = CacheBuilder::new(
            cache_manager,
            app_server_url,
            table_name.clone(),
            &endpoint,
            labels,
        )
        .await?;
        let cache_reader = cache_builder.cache_reader().clone();

        // Generate descriptor.
        let temp_dir = TempDir::new(&table_name).map_err(ApiInitError::CreateTempDir)?;
        let proto_folder_path = temp_dir.path();
        let descriptor_path = proto_folder_path.join("descriptor.bin");
        let descriptor = generate_all(
            proto_folder_path,
            &descriptor_path,
            [(table_name.as_str(), &endpoint_schema)],
        )?;

        // Start cache builder.
        let handle = {
            let operations_sender = operations_sender.map(|sender| (table_name.clone(), sender));
            let runtime_clone = runtime.clone();
            runtime_clone
                .spawn_blocking(move || cache_builder.run(runtime, cancel, operations_sender))
        };

        Ok((
            Self {
                cache_reader,
                descriptor,
                table_name,
                endpoint,
            },
            handle,
        ))
    }

    pub fn open(
        cache_manager: &dyn RwCacheManager,
        descriptor: Vec<u8>,
        table_name: String,
        endpoint: ApiEndpoint,
    ) -> Result<Self, ApiInitError> {
        let mut labels = Labels::new();
        labels.push(ENDPOINT_LABEL.to_string(), table_name.clone());
        Ok(Self {
            cache_reader: Arc::new(ArcSwap::from_pointee(open_existing_cache_reader(
                cache_manager,
                labels,
            )?)),
            descriptor,
            table_name,
            endpoint,
        })
    }

    pub fn cache_reader(&self) -> impl Deref<Target = Arc<CacheReader>> + '_ {
        self.cache_reader.load()
    }

    pub fn descriptor(&self) -> &[u8] {
        &self.descriptor
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn endpoint(&self) -> &ApiEndpoint {
        &self.endpoint
    }
}

pub fn cache_alias_and_labels(endpoint: String) -> (String, Labels) {
    let mut labels = Labels::new();
    labels.push(ENDPOINT_LABEL, endpoint);
    (labels.to_non_empty_string().into_owned(), labels)
}

fn open_cache_reader(
    cache_manager: &dyn RwCacheManager,
    labels: Labels,
) -> Result<Option<CacheReader>, ApiInitError> {
    let cache = cache_manager
        .open_ro_cache(labels.to_non_empty_string().into_owned(), labels)
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
pub mod shutdown;
pub use dozer_types::tonic;
use errors::ApiInitError;
pub use openapiv3;
pub use tokio;
use tokio::{runtime::Runtime, sync::broadcast::Sender, task::JoinHandle};
pub use tracing_actix_web;
#[cfg(test)]
mod test_utils;
