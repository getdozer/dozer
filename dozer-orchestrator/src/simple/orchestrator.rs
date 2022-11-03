use std::{sync::Arc, thread};

use dozer_api::api_server::ApiServer;
use dozer_cache::cache::LmdbCache;

use super::executor::Executor;
use crate::Orchestrator;
use crossbeam::channel::{self};
use dozer_api::grpc_server::GRPCServer;
use dozer_types::{
    errors::orchestrator::OrchestrationError,
    models::{
        api_endpoint::{APIConfig, ApiEndpoint},
        source::Source,
    },
};

#[derive(Default)]
pub struct SimpleOrchestrator {
    pub sources: Vec<Source>,
    pub api_endpoints: Vec<ApiEndpoint>,
    pub api_config: Option<APIConfig>,
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
        let endpoint3 = self.api_endpoints.get(0).unwrap().clone();

        let sources = self.sources.clone();
        let api_config = self.api_config.clone();
        let rest_config = api_config
            .clone()
            .map_or_else(|| None, |config| config.rest);
        let grpc_config = api_config
            .clone()
            .map_or_else(|| None, |config| config.grpc);

        let thread = thread::spawn(move || {
            let api_server = match rest_config {
                Some(rest_config) => ApiServer::new(rest_config.port, rest_config.security),
                None => ApiServer::default(),
            };
            api_server.run(endpoints, cache_2).unwrap()
        });
        let (sender, receiver) = channel::unbounded::<bool>();
        let _thread2 = thread::spawn(move || {
            // TODO: Refactor add endpoint method to support multiple endpoints
            Executor::run(sources, endpoints2, cache, sender).unwrap();
        });

        let schema_inserted = receiver.recv();
        if schema_inserted.is_ok() {
            let _thread3 = thread::spawn(move || {
                let grpc_server = match grpc_config {
                    Some(grpc_config) => GRPCServer::new(grpc_config.port, grpc_config.security),
                    None => GRPCServer::default(),
                };
                grpc_server.run(endpoint3, cache_3).unwrap()
            });
        }

        match thread.join() {
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
            api_config: None,
        }
    }
}
