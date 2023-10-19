use std::sync::Arc;
use std::time::Duration;

use crate::errors::ApiInitError;
use arc_swap::ArcSwap;
use dozer_cache::dozer_log::reader::{LogClient, LogReader, LogReaderOptions, OpAndPos};
use dozer_cache::dozer_log::schemas::EndpointSchema;
use dozer_cache::CacheReader;
use dozer_cache::{
    cache::{CacheWriteOptions, RwCacheManager},
    errors::CacheError,
};
use dozer_tracing::LabelsAndProgress;
use dozer_types::grpc_types::internal::internal_pipeline_service_client::InternalPipelineServiceClient;
use dozer_types::models::api_endpoint::{
    default_log_reader_batch_size, default_log_reader_buffer_size,
    default_log_reader_timeout_in_millis, ApiEndpoint, ConflictResolution,
};
use dozer_types::tonic::transport::Channel;
use dozer_types::{grpc_types::types::Operation as GrpcOperation, log::error};
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
    /// The cache that's being built. It must have the name of the connected log's id but may not be `serving`.
    state: CacheBuilderState,
    log_reader_options: LogReaderOptions,
    log_reader: LogReader,
}

mod endpoint_meta;
mod state;

use endpoint_meta::EndpointMeta;
use state::CacheBuilderState;

impl CacheBuilder {
    pub async fn new(
        cache_manager: Arc<dyn RwCacheManager>,
        app_server_url: String,
        endpoint: &ApiEndpoint,
        labels: LabelsAndProgress,
    ) -> Result<(Self, Vec<u8>), ApiInitError> {
        // Connect to the endpoint's log.
        let mut client = InternalPipelineServiceClient::connect(app_server_url.clone())
            .await
            .map_err(|error| ApiInitError::ConnectToAppServer {
                url: app_server_url,
                error,
            })?;
        let endpoint_meta =
            EndpointMeta::load_from_client(&mut client, endpoint.name.clone()).await?;

        // Open or create cache.
        let cache_write_options = cache_write_options(endpoint.conflict_resolution);
        let progress_bar = labels.create_progress_bar(format!("cache: {}", endpoint_meta.name));
        let state = CacheBuilderState::new(
            cache_manager,
            labels.labels().clone(),
            cache_write_options,
            endpoint_meta.clone(),
            progress_bar,
        )?;

        // Create log reader.
        let log_reader_options = get_log_reader_options(endpoint);
        let log_reader = create_log_reader(
            &mut client,
            endpoint_meta.schema,
            log_reader_options.clone(),
            state.next_log_position(),
        )
        .await?;

        Ok((
            Self {
                client,
                state,
                log_reader_options,
                log_reader,
            },
            endpoint_meta.descriptor_bytes,
        ))
    }

    pub fn cache_reader(&self) -> &Arc<ArcSwap<CacheReader>> {
        self.state.cache_reader()
    }

    pub fn run(
        mut self,
        runtime: Arc<Runtime>,
        mut cancel: impl Future<Output = ()> + Unpin + Send + 'static,
        operations_sender: Option<(String, Sender<GrpcOperation>)>,
    ) -> Result<(), CacheError> {
        loop {
            let Some(read_one_result) = runtime.block_on(self.read_one_with_cancel(cancel)) else {
                return Ok(());
            };
            cancel = read_one_result.1;

            self.state
                .process_op(read_one_result.0, operations_sender.as_ref())?;
        }
    }

    async fn read_one_with_cancel<F: Future<Output = ()> + Unpin>(
        &mut self,
        cancel: F,
    ) -> Option<(OpAndPos, F)> {
        let next_op = std::pin::pin!(self.read_one_until_success());
        match select(cancel, next_op).await {
            Either::Left(_) => None,
            Either::Right((op, c)) => Some((op, c)),
        }
    }

    async fn read_one_until_success(&mut self) -> OpAndPos {
        loop {
            match self.log_reader.read_one().await {
                Ok(op_and_pos) => return op_and_pos,
                Err(e) => {
                    error!("Failed to read log: {e}, reconnecting");
                    self.reconnect_until_success().await;
                }
            }
        }
    }

    async fn reconnect_until_success(&mut self) {
        loop {
            match self.reconnect().await {
                Ok(()) => return,
                Err(e) => {
                    error!("Failed to reconnect: {e}, retrying after {READ_LOG_RETRY_INTERVAL:?}");
                    tokio::time::sleep(READ_LOG_RETRY_INTERVAL).await;
                }
            }
        }
    }

    async fn reconnect(&mut self) -> Result<(), ApiInitError> {
        // Get endpoint meta.
        let endpoint_meta = EndpointMeta::load_from_client(
            &mut self.client,
            self.log_reader_options.endpoint.clone(),
        )
        .await?;
        // Compare cache and log id.
        self.state.update(endpoint_meta.clone())?;
        // Create log reader.
        self.log_reader = create_log_reader(
            &mut self.client,
            endpoint_meta.schema,
            self.log_reader_options.clone(),
            self.state.next_log_position(),
        )
        .await?;
        Ok(())
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

async fn create_log_reader(
    client: &mut InternalPipelineServiceClient<Channel>,
    schema: EndpointSchema,
    options: LogReaderOptions,
    start: u64,
) -> Result<LogReader, ApiInitError> {
    let client = LogClient::new(client, options.endpoint.clone()).await?;
    Ok(LogReader::new(schema, client, options, start))
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
