use std::path::PathBuf;
use std::{sync::Arc, thread};

use dozer_api::actix_web::dev::ServerHandle;
use dozer_api::grpc::{start_internal_client, start_internal_server};
use dozer_api::CacheEndpoint;
use dozer_cache::cache::{CacheOptions, CacheReadOptions, CacheWriteOptions, LmdbCache};
use dozer_ingestion::ingestion::{IngestionConfig, Ingestor};

use super::executor::{Executor, SinkConfig};
use crate::errors::OrchestrationError;
use crate::Orchestrator;
use dozer_api::grpc::internal_grpc::PipelineRequest;
use dozer_api::{grpc, rest};
use dozer_types::crossbeam::channel::{self, unbounded};
use dozer_types::models::{api_endpoint::ApiEndpoint, source::Source};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default, Clone)]
pub struct SimpleOrchestrator {
    pub sources: Vec<Source>,
    pub api_endpoints: Vec<ApiEndpoint>,
    pub cache_read_options: CacheReadOptions,
    pub cache_write_options: CacheWriteOptions,
    // Home directory where all files will be located
    pub home_dir: PathBuf,
    pub internal_port: u16,
}

impl SimpleOrchestrator {
    pub fn new(home_dir: PathBuf) -> Self {
        Self {
            home_dir,
            internal_port: 50052,
            ..Default::default()
        }
    }
}

impl Orchestrator for SimpleOrchestrator {
    fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self {
        for source in sources.iter() {
            self.sources.push(source.to_owned());
        }
        self
    }

    fn add_endpoints(&mut self, endpoints: Vec<ApiEndpoint>) -> &mut Self {
        self.api_endpoints = endpoints;
        self
    }

    fn run_api(&mut self, running: Arc<AtomicBool>) -> Result<(), OrchestrationError> {
        // Channel to communicate CtrlC with API Server
        let (tx, rx) = unbounded::<ServerHandle>();

        let running2 = running.clone();
        // gRPC notifier channel
        let (sender, receiver) = channel::unbounded::<PipelineRequest>();

        let cache_endpoints: Vec<CacheEndpoint> = self
            .api_endpoints
            .iter()
            .map(|e| {
                let mut cache_read_options = self.cache_read_options.clone();
                cache_read_options.set_path(self.home_dir.join(e.name.clone()));
                CacheEndpoint {
                    cache: Arc::new(
                        LmdbCache::new(CacheOptions::ReadOnly(cache_read_options)).unwrap(),
                    ),
                    endpoint: e.to_owned(),
                }
            })
            .collect();

        let ce2 = cache_endpoints.clone();
        let ce3 = cache_endpoints;

        let internal_port = self.internal_port;

        // Initialize Internal Server
        let _internal_thread = thread::spawn(move || -> Result<(), OrchestrationError> {
            start_internal_server(internal_port, sender)
                .map_err(|_e| OrchestrationError::InternalServerError)
        });

        // Initialize API Server
        let thread = thread::spawn(move || -> Result<(), OrchestrationError> {
            let api_server = rest::ApiServer::default();
            api_server
                .run(ce3, tx)
                .map_err(OrchestrationError::ApiServerFailed)
        });
        let server_handle = rx.recv().map_err(OrchestrationError::RecvError)?;

        // Initialize GRPC Server
        let grpc_server = grpc::ApiServer::new(receiver, 50051, false);

        let _grpc_thread = thread::spawn(move || -> Result<(), OrchestrationError> {
            grpc_server
                .run(ce2, running2.to_owned())
                .map_err(OrchestrationError::GrpcServerFailed)
                .unwrap();
            Ok(())
        });

        // Waiting for Ctrl+C
        while running.load(Ordering::SeqCst) {}
        rest::ApiServer::stop(server_handle);

        thread.join().unwrap()?;

        Ok(())
    }

    fn run_apps(&mut self, running: Arc<AtomicBool>) -> Result<(), OrchestrationError> {
        //Set AtomicBool and wait for CtrlC

        let executor_running = running.clone();
        // gRPC notifier channel
        let (sender, receiver) = channel::unbounded::<PipelineRequest>();

        let internal_port = self.internal_port;
        // Initialize Internal Server Client
        let _internal_thread = thread::spawn(move || {
            start_internal_client(internal_port, receiver);
        });

        // Ingestion Channe;
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

        let cache_endpoints: Vec<CacheEndpoint> = self
            .api_endpoints
            .iter()
            .map(|e| {
                let mut cache_write_options = self.cache_write_options.clone();
                cache_write_options.set_path(self.home_dir.join(e.name.clone()));
                CacheEndpoint {
                    cache: Arc::new(
                        LmdbCache::new(CacheOptions::Write(cache_write_options)).unwrap(),
                    ),
                    endpoint: e.to_owned(),
                }
            })
            .collect();
        let sources = self.sources.clone();

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
