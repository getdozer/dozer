use crate::Orchestrator;
use dozer_types::types::TableInfo;

use super::{
    models::{connection::Connection, endpoint::Endpoint, source::Source},
    services::connection::ConnectionService,
};

pub struct Simple {
    sources: Vec<Source>,
    endpoints: Vec<Endpoint>,
}

impl Orchestrator for Simple {
    fn test_connection(input: Connection) -> anyhow::Result<()> {
        let connection_service = ConnectionService::new(input);
        return connection_service.test_connection();
    }

    fn get_schema(input: Connection) -> anyhow::Result<Vec<TableInfo>> {
        let connection_service = ConnectionService::new(input);
        return connection_service.get_schema();
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

impl Simple {
    pub fn new() -> Self {
        Self {
            sources: vec![],
            endpoints: vec![],
        }
    }
}
