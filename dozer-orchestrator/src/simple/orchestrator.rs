use std::{sync::Arc, thread};

use dozer_api::server::ApiServer;
use dozer_cache::cache::lmdb::cache::LmdbCache;
use dozer_schema::registry::SchemaRegistryClient;
use tokio::runtime::Runtime;

use super::executor::Executor;
use crate::Orchestrator;
use dozer_types::models::{
    api_endpoint::{self, ApiEndpoint},
    source::Source,
};

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

        let api_endpoint = self.api_endpoint.as_ref().unwrap().clone();

        let cache_2 = cache.clone();

        Executor::run(&self, cache)?;

        let thread = thread::spawn(move || {
            let api_server = ApiServer::default();
            api_server.run(vec![api_endpoint], cache_2).unwrap();
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
