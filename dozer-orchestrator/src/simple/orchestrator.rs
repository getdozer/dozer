use super::executor::Executor;
use crate::Orchestrator;
use dozer_api::{api_server::ApiServer, grpc_server::GRPCServer};
use dozer_cache::cache::LmdbCache;
use dozer_schema::registry::SchemaRegistryClient;
use dozer_types::{
    errors::orchestrator::OrchestrationError,
    models::{api_endpoint::ApiEndpoint, source::Source},
};
use std::{sync::Arc, thread};

pub struct SimpleOrchestrator {
    pub sources: Vec<Source>,
    pub api_endpoints: Vec<ApiEndpoint>,
    pub schema_client: Arc<SchemaRegistryClient>,
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

        let thread = thread::spawn(move || {
            let api_server = ApiServer::default();
            api_server.run(endpoints, cache_2).unwrap()
        });
        let _thread3 = thread::spawn(move || {
            //TODO: Have to wait until cache is initialized and schema is exist => GRPC have to know schema before starting
            thread::sleep(std::time::Duration::from_millis(1000));
            let grpc_server = GRPCServer::default();
            grpc_server.run(endpoints3, cache_3).unwrap()
        });

        let _thread2 = thread::spawn(move || {
            // TODO: Refactor add endpoint method to support multiple endpoints
            Executor::run(sources, endpoints2, cache).unwrap();
        });

        match thread.join() {
            Ok(_) => Ok(()),
            Err(_) => Err(OrchestrationError::InitializationFailed),
        }?;

        Ok(())
    }
}

impl SimpleOrchestrator {
    pub fn new(schema_client: Arc<SchemaRegistryClient>) -> Self {
        Self {
            sources: vec![],
            api_endpoints: vec![],
            schema_client,
        }
    }
}
