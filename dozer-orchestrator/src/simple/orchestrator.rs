use dozer_api::api_server::ApiServer;
use dozer_api::grpc_server::GRPCServer;
use dozer_cache::cache::LmdbCache;
use dozer_schema::registry::SchemaRegistryClient;
use std::{sync::Arc, thread};

use super::executor::Executor;
use crate::Orchestrator;
use dozer_types::models::{api_endpoint::ApiEndpoint, source::Source};

pub struct SimpleOrchestrator {
    pub sources: Vec<Source>,
    pub api_endpoint: Option<ApiEndpoint>,
    pub schema_client: Arc<SchemaRegistryClient>,
}

impl Orchestrator for SimpleOrchestrator {
    fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self {
        for source in sources.iter() {
            self.sources.push(source.to_owned());
        }
        self
    }

    fn add_endpoint(&mut self, endpoint: ApiEndpoint) -> &mut Self {
        self.api_endpoint = Some(endpoint);
        self
    }

    fn run(&mut self) -> anyhow::Result<()> {
        let cache = Arc::new(LmdbCache::new(true));
        let cache_2 = cache.clone();
        let cache_3 = cache.clone();

        let api_endpoint = self.api_endpoint.as_ref().unwrap().clone();
        let api_endpoint2 = self.api_endpoint.as_ref().unwrap().clone();
        let api_endpoint3 = self.api_endpoint.as_ref().unwrap().clone();

        let sources = self.sources.clone();

        let thread = thread::spawn(move || {
            let api_server = ApiServer::default();
            api_server.run(vec![api_endpoint], cache_2).unwrap()
        });

        let _thread2 = thread::spawn(move || {
            Executor::run(sources, api_endpoint2, cache).unwrap();
        });
        let _thread3 = thread::spawn(move || {
            let grpc_server = GRPCServer::default();
            grpc_server.run(vec![api_endpoint3], cache_3, "film".to_string())
        });
        thread.join().unwrap();

        Ok(())
    }
}

impl SimpleOrchestrator {
    pub fn new(schema_client: Arc<SchemaRegistryClient>) -> Self {
        Self {
            sources: vec![],
            api_endpoint: None,
            schema_client,
        }
    }
}
