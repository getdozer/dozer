pub mod models;
pub mod pipeline;
mod services;
pub mod simple;
use dozer_types::types::Schema;
use services::connection::ConnectionService;

use crate::models::{api_endpoint::ApiEndpoint, connection::Connection, source::Source};

pub trait Orchestrator {
    fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self;
    fn add_endpoint(&mut self, endpoint: ApiEndpoint) -> &mut Self;
    fn run(&mut self) -> anyhow::Result<()>;
}

pub fn test_connection(input: Connection) -> anyhow::Result<()> {
    let connection_service = ConnectionService::new(input);
    return connection_service.test_connection();
}

pub fn get_schema(input: Connection) -> Result<Vec<(String, Schema)>, anyhow::Error> {
    let connection_service = ConnectionService::new(input);
    return connection_service.get_all_schema();
}
