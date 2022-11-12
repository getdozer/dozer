use std::{sync::Arc, thread};

use dozer_api::CacheEndpoint;
use dozer_api::{actix_web::dev::ServerHandle, api_server::ApiServer};
use dozer_cache::cache::LmdbCache;
use dozer_ingestion::ingestion::{IngestionConfig, Ingestor};
use dozer_types::events::ApiEvent;

use super::executor::Executor;
use crate::errors::OrchestrationError;
use crate::Orchestrator;
use dozer_api::grpc_server::GRPCServer;
use dozer_types::crossbeam::channel::{self, unbounded};
use dozer_types::models::{api_endpoint::ApiEndpoint, source::Source};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub struct SimpleOrchestrator {
    pub sources: Vec<Source>,
    pub api_endpoints: Vec<ApiEndpoint>,
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

    fn run(&mut self) -> Result<(), OrchestrationError> {
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
                cache: Arc::new(LmdbCache::new(true)),
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

impl SimpleOrchestrator {
    pub fn new() -> Self {
        Self {
            sources: vec![],
            api_endpoints: vec![],
        }
    }
}
