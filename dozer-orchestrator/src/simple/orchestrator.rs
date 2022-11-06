use std::{sync::Arc, thread};

use dozer_api::CacheEndpoint;
use dozer_api::{actix_web::dev::ServerHandle, api_server::ApiServer};
use dozer_cache::cache::LmdbCache;
use log::debug;

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
        let cache_endpoint = cache_endpoints.get(0).unwrap().clone();
        let ce2 = cache_endpoint.clone();
        let ce3 = cache_endpoint.clone();

        let sources = self.sources.clone();
        let (tx, rx) = unbounded::<ServerHandle>();

        // Initialize Pipeline
        let (sender, receiver) = channel::unbounded::<bool>();
        let thread2 = thread::spawn(move || -> Result<(), OrchestrationError> {
            // TODO: Refactor add endpoint method to support multiple endpoints
            Executor::run(sources, cache_endpoint, sender, running2)?;
            Ok(())
        });

        // Initialize API Server
        let thread = thread::spawn(move || -> Result<(), OrchestrationError> {
            let api_server = ApiServer::default();
            api_server
                .run(vec![ce2.endpoint], ce2.cache, tx)
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
                .run(ce3.endpoint, ce3.cache, running3)
                .map_err(OrchestrationError::GrpcServerFailed)
        });

        // Waiting for Ctrl+C
        while running.load(Ordering::SeqCst) {}
        ApiServer::stop(server_handle);

        thread2.join().unwrap()?;
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
