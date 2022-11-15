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
    fn run(&mut self) -> Result<(), OrchestrationError>;
}

// Re-exports
pub use dozer_ingestion::connectors::{get_connector, Connector, TableInfo};
