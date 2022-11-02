use std::{sync::Arc, thread};

use dozer_api::api_server::ApiServer;
use dozer_cache::cache::LmdbCache;

use super::executor::Executor;
use crate::Orchestrator;
use crossbeam::channel::{self};
use dozer_api::grpc_server::GRPCServer;
use dozer_types::{
    errors::orchestrator::OrchestrationError,
    events::Event,
    models::{api_endpoint::ApiEndpoint, source::Source},
};

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
        let cache = Arc::new(LmdbCache::new(true));
        let cache_2 = cache.clone();
        let cache_3 = cache.clone();

        let endpoints = self.api_endpoints.clone();
        let endpoints2 = self.api_endpoints.get(0).unwrap().clone();
        let endpoints3 = self.api_endpoints.get(0).unwrap().clone();

        let sources = self.sources.clone();

        let api_thread = thread::spawn(move || {
            let api_server = ApiServer::default();
            api_server.run(endpoints, cache_2)
        });
        let (sender, receiver) = channel::unbounded::<Event>();
        let receiver1 = receiver;
        let _executor_thread = thread::spawn(move || {
            // TODO: Refactor add endpoint method to support multiple endpoints
            _ = Executor::run(sources, endpoints2, cache, sender);
        });

        let grpc_server = GRPCServer::new(receiver1, 50051);
        let _grpc_thread = thread::spawn(move || {
            _ = grpc_server.run(endpoints3, cache_3);
        });

        match api_thread.join() {
            Ok(_) => Ok(()),
            Err(_) => Err(OrchestrationError::InitializationFailed),
        }?;
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
