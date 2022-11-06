use std::{sync::Arc, thread};

use dozer_api::{actix_web::dev::ServerHandle, api_server::ApiServer};
use dozer_cache::cache::LmdbCache;

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

        ctrlc::set_handler(move || {
            r.store(false, Ordering::SeqCst);
        })
        .expect("Error setting Ctrl-C handler");

        let cache = Arc::new(LmdbCache::new(true));
        let cache_2 = cache.clone();
        let cache_3 = cache.clone();

        let endpoints = self.api_endpoints.clone();
        let endpoints2 = self.api_endpoints.get(0).unwrap().clone();
        let endpoint3 = self.api_endpoints.get(0).unwrap().clone();

        let sources = self.sources.clone();
        let (tx, rx) = unbounded::<ServerHandle>();

        // Initialize Pipeline
        let (sender, receiver) = channel::unbounded::<bool>();
        let _thread2 = thread::spawn(move || -> Result<(), OrchestrationError> {
            // TODO: Refactor add endpoint method to support multiple endpoints
            Executor::run(sources, endpoints2, cache, sender)?;
            Ok(())
        });

        // Initialize API Server
        let _thread = thread::spawn(move || -> Result<(), OrchestrationError> {
            let api_server = ApiServer::default();
            api_server
                .run(endpoints, cache_2, tx)
                .map_err(OrchestrationError::ApiServerFailed)
        });
        let server_handle = rx.recv().map_err(OrchestrationError::RecvError)?;

        // Initialize GRPC Server
        let _thread3 = thread::spawn(move || -> Result<(), OrchestrationError> {
            receiver
                .recv()
                .map_err(OrchestrationError::SchemaUpdateFailed)?;
            let grpc_server = GRPCServer::default();
            grpc_server
                .run(endpoint3, cache_3)
                .map_err(OrchestrationError::GrpcServerFailed)
        });

        // Waiting for Ctrl+C
        while running.load(Ordering::SeqCst) {}

        ApiServer::stop(server_handle);

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
