pub mod errors;
pub mod pipeline;
mod services;
pub mod simple;

use dozer_ingestion::errors::ConnectorError;
use dozer_types::types::Schema;
use errors::OrchestrationError;
use services::connection::ConnectionService;

use dozer_types::models::{api_endpoint::ApiEndpoint, connection::Connection, source::Source};

#[cfg(test)]
mod test_utils;

pub trait Orchestrator {
    fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self;
    fn add_endpoints(&mut self, endpoint: Vec<ApiEndpoint>) -> &mut Self;
    fn run(&mut self) -> Result<(), OrchestrationError>;
}

pub fn test_connection(input: Connection) -> Result<(), ConnectorError> {
    let connection_service = ConnectionService::new(input);
    connection_service.test_connection()
}

pub fn get_schema(input: Connection) -> Result<Vec<(String, Schema)>, ConnectorError> {
    let connection_service = ConnectionService::new(input);
    connection_service.get_all_schema()
}
