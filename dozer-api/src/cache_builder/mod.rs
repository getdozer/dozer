use std::sync::Arc;
use std::time::Duration;

use crate::errors::ApiInitError;
use arc_swap::ArcSwap;
use dozer_cache::cache::{RoCache, RwCache};
use dozer_cache::dozer_log::reader::{LogClient, LogReader, LogReaderOptions};
use dozer_cache::dozer_log::schemas::EndpointSchema;
use dozer_cache::CacheReader;
use dozer_cache::{
    cache::{CacheWriteOptions, RwCacheManager},
    errors::CacheError,
};
use dozer_services::internal::internal_pipeline_service_client::InternalPipelineServiceClient;
use dozer_services::tonic::transport::Channel;
use dozer_services::types::Operation as GrpcOperation;
use dozer_tracing::{Labels, LabelsAndProgress};
use dozer_types::indicatif::ProgressBar;
use dozer_types::log::error;
use dozer_types::models::api_endpoint::{
    default_log_reader_batch_size, default_log_reader_buffer_size,
    default_log_reader_timeout_in_millis, ApiEndpoint, ConflictResolution,
};
use dozer_types::types::SchemaWithIndex;
use futures_util::{
    future::{select, Either},
    Future,
};
use tokio::runtime::Runtime;
use tokio::sync::broadcast::Sender;

const READ_LOG_RETRY_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub struct CacheBuilder {
    client: InternalPipelineServiceClient<Channel>,
    cache_manager: Arc<dyn RwCacheManager>,
    serving: Arc<ArcSwap<CacheReader>>,
    labels: Labels,
    cache_write_options: CacheWriteOptions,
    progress_bar: ProgressBar,
    log_reader_options: LogReaderOptions,
}

mod builder_impl;
mod endpoint_meta;

use builder_impl::CacheBuilderImpl;
use endpoint_meta::EndpointMeta;

impl CacheBuilder {
    pub async fn new(
        cache_manager: Arc<dyn RwCacheManager>,
        app_server_url: String,
        endpoint: &ApiEndpoint,
        labels: LabelsAndProgress,
    ) -> Result<(Self, EndpointSchema), ApiInitError> {
        // Connect to the endpoint's log.
        let mut client = InternalPipelineServiceClient::connect(app_server_url.clone())
            .await
            .map_err(|error| ApiInitError::ConnectToAppServer {
                url: app_server_url,
                error,
            })?;
        let (endpoint_meta, _) =
            EndpointMeta::load_from_client(&mut client, endpoint.name.clone()).await?;

        // Open or create cache.
        let cache_write_options = cache_write_options(endpoint.conflict_resolution);
        let serving = open_or_create_cache_reader(
            &*cache_manager,
            endpoint_meta.clone(),
            labels.labels().clone(),
            cache_write_options,
        )
        .map_err(ApiInitError::OpenOrCreateCache)?;
        let progress_bar = labels.create_progress_bar(format!("cache: {}", endpoint_meta.name));

        let log_reader_options = get_log_reader_options(endpoint);

        Ok((
            Self {
                client,
                cache_manager,
                serving: Arc::new(ArcSwap::from_pointee(serving)),
                labels: labels.labels().clone(),
                cache_write_options,
                progress_bar,
                log_reader_options,
            },
            endpoint_meta.schema,
        ))
    }

    pub fn cache_reader(&self) -> &Arc<ArcSwap<CacheReader>> {
        &self.serving
    }

    pub fn run(
        mut self,
        runtime: Arc<Runtime>,
        mut cancel: impl Future<Output = ()> + Unpin + Send + 'static,
        operations_sender: Option<(String, Sender<GrpcOperation>)>,
    ) -> Result<(), CacheError> {
        loop {
            // Connect to the endpoint's log.
            let Some(connect_result) = runtime.block_on(with_cancel(
                connect_until_success(&mut self.client, &self.log_reader_options.endpoint),
                cancel,
            )) else {
                return Ok(());
            };
            cancel = connect_result.1;

            // Create `CacheBuilderImpl` and `LogReader`.
            let (endpoint_meta, log_client) = connect_result.0;
            let (mut builder, start) = CacheBuilderImpl::new(
                self.cache_manager.clone(),
                self.serving.clone(),
                endpoint_meta.clone(),
                self.labels.clone(),
                self.cache_write_options,
                self.progress_bar.clone(),
            )?;
            let mut log_reader = LogReader::new(
                endpoint_meta.schema,
                log_client,
                self.log_reader_options.clone(),
                start,
            );

            // Loop over the log until error.
            loop {
                let Some(read_one_result) =
                    runtime.block_on(with_cancel(log_reader.read_one(), cancel))
                else {
                    return Ok(());
                };
                cancel = read_one_result.1;

                match read_one_result.0 {
                    Ok(op_and_pos) => {
                        builder.process_op(op_and_pos, operations_sender.as_ref())?;
                    }
                    Err(e) => {
                        error!("Failed to read log: {e}, reconnecting");
                        break;
                    }
                }
            }
        }
    }
}

async fn connect_until_success(
    client: &mut InternalPipelineServiceClient<Channel>,
    endpoint: &str,
) -> (EndpointMeta, LogClient) {
    loop {
        match EndpointMeta::load_from_client(client, endpoint.to_string()).await {
            Ok(endpoint_meta_and_log_client) => return endpoint_meta_and_log_client,
            Err(e) => {
                error!("Failed to reconnect: {e}, retrying after {READ_LOG_RETRY_INTERVAL:?}");
                tokio::time::sleep(READ_LOG_RETRY_INTERVAL).await;
            }
        }
    }
}

async fn with_cancel<T, F: Future<Output = T>, C: Future<Output = ()> + Unpin>(
    future: F,
    cancel: C,
) -> Option<(T, C)> {
    let future = std::pin::pin!(future);
    match select(cancel, future).await {
        Either::Left(_) => None,
        Either::Right((result, c)) => Some((result, c)),
    }
}

fn cache_write_options(conflict_resolution: ConflictResolution) -> CacheWriteOptions {
    CacheWriteOptions {
        insert_resolution: conflict_resolution.on_insert,
        delete_resolution: conflict_resolution.on_delete,
        update_resolution: conflict_resolution.on_update,
        ..Default::default()
    }
}

fn open_or_create_cache_reader(
    cache_manager: &dyn RwCacheManager,
    endpoint_meta: EndpointMeta,
    labels: Labels,
    write_options: CacheWriteOptions,
) -> Result<CacheReader, CacheError> {
    if let Some(reader) = open_cache_reader(cache_manager, endpoint_meta.clone(), labels.clone())? {
        Ok(reader)
    } else {
        create_cache(
            cache_manager,
            endpoint_meta.clone(),
            labels.clone(),
            write_options,
        )?;
        Ok(open_cache_reader(cache_manager, endpoint_meta, labels)?
            .expect("cache was just created"))
    }
}

fn open_cache(
    cache_manager: &dyn RwCacheManager,
    endpoint_meta: EndpointMeta,
    labels: Labels,
) -> Result<Option<Box<dyn RwCache>>, CacheError> {
    let (alias, cache_labels) = endpoint_meta.cache_alias_and_labels(labels);
    let cache = cache_manager.open_rw_cache(alias.clone(), cache_labels, Default::default())?;
    if let Some(cache) = cache.as_ref() {
        check_cache_schema(
            cache.as_ro(),
            (
                endpoint_meta.schema.schema,
                endpoint_meta.schema.secondary_indexes,
            ),
        )?;
    }
    Ok(cache)
}

fn create_cache(
    cache_manager: &dyn RwCacheManager,
    endpoint_meta: EndpointMeta,
    labels: Labels,
    write_options: CacheWriteOptions,
) -> Result<Box<dyn RwCache>, CacheError> {
    let (alias, cache_labels) = endpoint_meta.cache_alias_and_labels(labels);
    let cache_name = endpoint_meta.cache_name();
    let cache = cache_manager.create_cache(
        cache_name.clone(),
        cache_labels,
        (
            endpoint_meta.schema.schema,
            endpoint_meta.schema.secondary_indexes,
        ),
        &endpoint_meta.schema.connections,
        write_options,
    )?;
    cache_manager.create_alias(&cache_name, &alias)?;
    Ok(cache)
}

fn open_cache_reader(
    cache_manager: &dyn RwCacheManager,
    endpoint_meta: EndpointMeta,
    labels: Labels,
) -> Result<Option<CacheReader>, CacheError> {
    let (alias, cache_labels) = endpoint_meta.cache_alias_and_labels(labels);
    let cache = cache_manager.open_ro_cache(alias.clone(), cache_labels)?;
    if let Some(cache) = cache.as_ref() {
        check_cache_schema(
            &**cache,
            (
                endpoint_meta.schema.schema,
                endpoint_meta.schema.secondary_indexes,
            ),
        )?;
    }
    Ok(cache.map(CacheReader::new))
}

fn check_cache_schema(cache: &dyn RoCache, given: SchemaWithIndex) -> Result<(), CacheError> {
    let stored = cache.get_schema();
    if &given != stored {
        return Err(CacheError::SchemaMismatch {
            name: cache.name().to_string(),
            given: Box::new(given),
            stored: Box::new(stored.clone()),
        });
    }
    Ok(())
}

fn get_log_reader_options(endpoint: &ApiEndpoint) -> LogReaderOptions {
    LogReaderOptions {
        endpoint: endpoint.name.clone(),
        batch_size: endpoint
            .log_reader_options
            .batch_size
            .unwrap_or_else(default_log_reader_batch_size),
        timeout_in_millis: endpoint
            .log_reader_options
            .timeout_in_millis
            .unwrap_or_else(default_log_reader_timeout_in_millis),
        buffer_size: endpoint
            .log_reader_options
            .buffer_size
            .unwrap_or_else(default_log_reader_buffer_size),
    }
}
