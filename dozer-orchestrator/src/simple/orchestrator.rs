use dozer_types::types::Schema;

use super::super::{
    models::{connection::Connection, endpoint::Endpoint, source::Source},
    services::connection::ConnectionService,
};
use crate::Orchestrator;

pub struct SimpleOrchestrator {
    sources: Vec<Source>,
    endpoints: Vec<Endpoint>,
    sql: Option<String>,
}

impl Orchestrator for SimpleOrchestrator {
    fn test_connection(input: Connection) -> anyhow::Result<()> {
        let connection_service = ConnectionService::new(input);
        return connection_service.test_connection();
    }

    fn get_schema(input: Connection) -> Result<Vec<(String, Schema)>, anyhow::Error> {
        let connection_service = ConnectionService::new(input);
        return connection_service.get_all_schema();
    }

    fn sql(&mut self, sql: String) -> &mut Self {
        self.sql = Some(sql);
        self
    }

    fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self {
        for source in sources.iter() {
            self.sources.push(source.to_owned());
        }
        self
    }

    fn add_endpoints(&mut self, endpoints: Vec<Endpoint>) -> &mut Self {
        for endpoint in endpoints.iter() {
            self.endpoints.push(endpoint.to_owned());
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
            endpoints: vec![],
            sql: None,
        }
    }
}
