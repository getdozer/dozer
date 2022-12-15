use super::executor::{Executor, SinkConfig};
use crate::errors::OrchestrationError;
use crate::internal::internal_pipeline_server::start_internal_pipeline_server;
use crate::Orchestrator;
use dozer_api::actix_web::dev::ServerHandle;
use dozer_api::grpc::internal_grpc::PipelineRequest;
use dozer_api::grpc::{start_internal_api_client, start_internal_api_server};
use dozer_api::CacheEndpoint;
use dozer_api::{grpc, rest};
use dozer_cache::cache::{CacheCommonOptions, CacheOptions, CacheReadOptions, CacheWriteOptions};
use dozer_cache::cache::{CacheOptionsKind, LmdbCache};
use dozer_ingestion::ingestion::IngestionConfig;
use dozer_ingestion::ingestion::Ingestor;
use dozer_types::crossbeam::channel::{self, unbounded, Sender};
use dozer_types::models::api_config::ApiConfig;
use dozer_types::models::app_config::Config;
use dozer_types::models::{api_endpoint::ApiEndpoint, source::Source};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{sync::Arc, thread};
use tokio::sync::oneshot;
#[derive(Default, Clone)]
pub struct SimpleOrchestrator {
    pub config: Config,
    pub cache_common_options: CacheCommonOptions,
    pub cache_read_options: CacheReadOptions,
    pub cache_write_options: CacheWriteOptions,
    // Home directory where all files will be located
    pub home_dir: PathBuf,
}

impl SimpleOrchestrator {
    pub fn new(home_dir: PathBuf, config: Config) -> Self {
        Self {
            home_dir,
            config,
            ..Default::default()
        }
    }
}

impl Orchestrator for SimpleOrchestrator {
    fn add_api_config(&mut self, api_config: ApiConfig) -> &mut Self {
        self.config.api = Some(api_config);
        self
    }

    fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self {
        for source in sources.iter() {
            self.config.sources.push(source.to_owned());
        }
        self
    }

    fn add_endpoints(&mut self, endpoints: Vec<ApiEndpoint>) -> &mut Self {
        for endpoint in endpoints.iter() {
            self.config.endpoints.push(endpoint.to_owned());
        }
        self
    }

    fn run_api(&mut self, running: Arc<AtomicBool>) -> Result<(), OrchestrationError> {
        // Channel to communicate CtrlC with API Server
        let (tx, rx) = unbounded::<ServerHandle>();
        let running2 = running.clone();
        // gRPC notifier channel
        let (sender, receiver) = channel::unbounded::<PipelineRequest>();

        let cache_endpoints: Vec<CacheEndpoint> = self
            .config
            .endpoints
            .iter()
            .map(|e| {
                let mut cache_common_options = self.cache_common_options.clone();
                cache_common_options.set_path(self.home_dir.join(e.name.clone()));
                CacheEndpoint {
                    cache: Arc::new(
                        LmdbCache::new(CacheOptions {
                            common: cache_common_options,
                            kind: CacheOptionsKind::ReadOnly(self.cache_read_options.clone()),
                        })
                        .unwrap(),
                    ),
                    endpoint: e.to_owned(),
                }
            })
            .collect();

        let ce2 = cache_endpoints.clone();
        let ce3 = cache_endpoints;

        let app_config = self.config.to_owned();
        let rt = tokio::runtime::Runtime::new().expect("Failed to initialize tokio runtime");
        let (sender_shutdown, receiver_shutdown) = oneshot::channel::<()>();
        rt.block_on(async {
            // Initialize Internal Server
            tokio::spawn(async move {
                start_internal_api_server(app_config, sender)
                    .await
                    .expect("Failed to initialize internal server")
            });

            // Initialize API Server
            let rest_config = self
                .config
                .to_owned()
                .api
                .unwrap_or_default()
                .rest
                .unwrap_or_default();
            tokio::spawn(async move {
                let api_server = rest::ApiServer::new(rest_config);
                api_server
                    .run(ce3, tx)
                    .await
                    .expect("Failed to initialize api server")
            });

            // Initialize GRPC Server
            let grpc_config = self
                .config
                .to_owned()
                .api
                .unwrap_or_default()
                .grpc
                .unwrap_or_default();
            let grpc_server = grpc::ApiServer::new(receiver, grpc_config, true);
            tokio::spawn(async move {
                grpc_server
                    .run(ce2, running2.to_owned(), receiver_shutdown)
                    .await
                    .expect("Failed to initialize gRPC server")
            });
        });

        let server_handle = rx.recv().map_err(OrchestrationError::RecvError)?;

        // Waiting for Ctrl+C
        while running.load(Ordering::SeqCst) {}
        sender_shutdown.send(()).unwrap();
        rest::ApiServer::stop(server_handle);

        Ok(())
    }

    fn run_apps(
        &mut self,
        running: Arc<AtomicBool>,
        api_notifier: Option<Sender<bool>>,
    ) -> Result<(), OrchestrationError> {
        let executor_running = running.clone();
        // gRPC notifier channel
        let (sender, receiver) = channel::unbounded::<PipelineRequest>();

        let internal_api_config = self
            .config
            .to_owned()
            .api
            .unwrap_or_default()
            .api_internal
            .unwrap_or_default();
        // Initialize Internal Server Client
        let _internal_api_thread = thread::spawn(move || {
            start_internal_api_client(internal_api_config, receiver);
        });

        let internal_app_config = self.config.to_owned();
        let _intern_pipeline_thread = thread::spawn(move || {
            _ = start_internal_pipeline_server(internal_app_config);
        });
        // Ingestion Channe;
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

        let cache_endpoints: Vec<CacheEndpoint> = self
            .config
            .endpoints
            .iter()
            .map(|e| {
                let mut cache_common_options = self.cache_common_options.clone();
                cache_common_options.set_path(self.home_dir.join(e.name.clone()));
                CacheEndpoint {
                    cache: Arc::new(
                        LmdbCache::new(CacheOptions {
                            common: cache_common_options,
                            kind: CacheOptionsKind::Write(self.cache_write_options.clone()),
                        })
                        .unwrap(),
                    ),
                    endpoint: e.to_owned(),
                }
            })
            .collect();

        if let Some(api_notifier) = api_notifier {
            api_notifier
                .send(true)
                .expect("Failed to notify API server");
        }

        let sources = self.config.sources.clone();

        let executor = Executor::new(
            sources,
            cache_endpoints,
            ingestor,
            iterator,
            running,
            self.home_dir.to_owned(),
            SinkConfig::default(),
        );
        executor.run(Some(sender), executor_running)
    }
}
