use std::path::Path;
use std::{sync::Arc, thread};

use dozer_api::CacheEndpoint;
use dozer_api::{actix_web::dev::ServerHandle, api_server::ApiServer};
use dozer_cache::cache::{CacheOptions, CacheReadOptions, CacheWriteOptions, LmdbCache};
use dozer_ingestion::ingestion::{IngestionConfig, Ingestor};
use dozer_types::events::ApiEvent;

use super::executor::Executor;
use crate::errors::OrchestrationError;
use crate::Orchestrator;
use dozer_api::client_server::GRPCServer;
use dozer_api::grpc::internal_grpc::PipelineRequest;
use dozer_types::crossbeam::channel::{self, unbounded};
use dozer_types::models::{api_endpoint::ApiEndpoint, source::Source};
use std::sync::atomic::{AtomicBool, Ordering};

pub struct SimpleOrchestrator {
    pub sources: Vec<Source>,
    pub api_endpoints: Vec<ApiEndpoint>,
    pub cache_read_options: CacheReadOptions,
    pub cache_write_options: CacheWriteOptions,
    // Home directory where all files will be located
    pub home_dir: Path,
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

    fn run_api(&mut self) -> Result<(), OrchestrationError> {
        //Set AtomicBool and wait for CtrlC
        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();
        let running2 = running.clone();
        let running3 = running.clone();

        // Channel to communicate CtrlC with API Server
        let (tx, rx) = unbounded::<ServerHandle>();

        // gRPC notifier channel
        let (sender, receiver) = channel::unbounded::<PipelineRequest>();

        ctrlc::set_handler(move || {
            r.store(false, Ordering::SeqCst);
        })
        .expect("Error setting Ctrl-C handler");

        let cache_endpoints: Vec<CacheEndpoint> = self
            .api_endpoints
            .iter()
            .map(|e| {
                let mut cache_read_options = self.cache_read_options;
                cache_read_options.set_path(self.home_dir.join(e.name));
                CacheEndpoint {
                    cache: Arc::new(
                        LmdbCache::new(CacheOptions::ReadOnly(cache_read_options)).unwrap(),
                    ),
                    endpoint: e.to_owned(),
                }
            })
            .collect();

        let ce2 = cache_endpoints.clone();
        let ce3 = cache_endpoints.clone();
        let sources = self.sources.clone();

        // Initialize API Server
        let thread = thread::spawn(move || -> Result<(), OrchestrationError> {
            let api_server = ApiServer::default();
            api_server
                .run(ce3, tx)
                .map_err(OrchestrationError::ApiServerFailed)
        });
        let server_handle = rx.recv().map_err(OrchestrationError::RecvError)?;

        // Initialize GRPC Server
        let grpc_server = GRPCServer::new(receiver, 50051, false);

        let _grpc_thread = thread::spawn(move || -> Result<(), OrchestrationError> {
            grpc_server
                .run(ce2, running3.to_owned())
                .map_err(OrchestrationError::GrpcServerFailed)
                .unwrap();
            Ok(())
        });

        // Waiting for Ctrl+C
        while running.load(Ordering::SeqCst) {}
        ApiServer::stop(server_handle);

        thread.join().unwrap()?;

        Ok(())
    }

    fn run_apps(&mut self) -> Result<(), OrchestrationError> {
        //Set AtomicBool and wait for CtrlC
        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();
        let running2 = running.clone();
        let running3 = running.clone();

        // Channel to communicate CtrlC with API Server
        let (tx, rx) = unbounded::<ServerHandle>();

        // gRPC notifier channel
        let (sender, receiver) = channel::unbounded::<ApiEvent>();

        // Ingestion Channe;
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

        ctrlc::set_handler(move || {
            r.store(false, Ordering::SeqCst);
        })
        .expect("Error setting Ctrl-C handler");

        let cache_endpoints: Vec<CacheEndpoint> = self
            .api_endpoints
            .iter()
            .map(|e| CacheEndpoint {
                cache: Arc::new(LmdbCache::new(CacheOptions::default()).unwrap()),
                endpoint: e.to_owned(),
            })
            .collect();

        let ce2 = cache_endpoints.clone();
        let ce3 = cache_endpoints.clone();
        let sources = self.sources.clone();

        let _thread2 = thread::spawn(move || -> Result<(), OrchestrationError> {
            let executor = Executor::new(sources, cache_endpoints, ingestor, iterator);
            executor.run(Some(sender), running2).unwrap();
            Ok(())
        });

        // Initialize API Server
        let thread = thread::spawn(move || -> Result<(), OrchestrationError> {
            let api_server = ApiServer::default();
            api_server
                .run(ce3, tx)
                .map_err(OrchestrationError::ApiServerFailed)
        });
        let server_handle = rx.recv().map_err(OrchestrationError::RecvError)?;

        // Initialize GRPC Server
        let grpc_server = GRPCServer::new(receiver, 50051, false);

        let _grpc_thread = thread::spawn(move || -> Result<(), OrchestrationError> {
            grpc_server
                .run(ce2, running3.to_owned())
                .map_err(OrchestrationError::GrpcServerFailed)
                .unwrap();
            Ok(())
        });

        // Waiting for Ctrl+C
        while running.load(Ordering::SeqCst) {}
        ApiServer::stop(server_handle);

        thread.join().unwrap()?;

        Ok(())
    }
}
