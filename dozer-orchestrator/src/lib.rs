pub mod errors;
pub mod pipeline;
pub mod simple;
use dozer_types::models::{api_endpoint::ApiEndpoint, source::Source};
use errors::OrchestrationError;

#[cfg(test)]
mod test_utils;

pub trait Orchestrator {
    fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self;
    fn add_endpoints(&mut self, endpoint: Vec<ApiEndpoint>) -> &mut Self;
    fn run_api(&mut self) -> Result<(), OrchestrationError>;
    fn run_apps(&mut self) -> Result<(), OrchestrationError>;
}

// Re-exports
pub use dozer_ingestion::connectors::{get_connector, Connector, TableInfo};
use dozer_ingestion::errors::ConnectorError;
use dozer_types::models::connection::Connection;

pub fn validate(input: Connection) -> Result<(), ConnectorError> {
    let connection_service = get_connector(input)?;
    connection_service.validate()
}
