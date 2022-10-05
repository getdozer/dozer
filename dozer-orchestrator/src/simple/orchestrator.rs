use super::super::models::{api_endpoint::ApiEndpoint, source::Source};
use crate::Orchestrator;

pub struct SimpleOrchestrator {
    sources: Vec<Source>,
    api_endpoints: Vec<ApiEndpoint>,
}

impl Orchestrator for SimpleOrchestrator {
    fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self {
        for source in sources.iter() {
            self.sources.push(source.to_owned());
        }
        self
    }

    fn add_endpoints(&mut self, endpoints: Vec<ApiEndpoint>) -> &mut Self {
        for api_endpoint in endpoints.iter() {
            self.api_endpoints.push(api_endpoint.to_owned());
        }
        self
    }

    fn run(&mut self) -> anyhow::Result<&mut Self> {
        todo!()
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
