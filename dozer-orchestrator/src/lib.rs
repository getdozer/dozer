pub mod cli;
pub mod errors;
pub mod internal;
pub mod pipeline;
pub mod simple;
pub use dozer_api::grpc::internal_grpc;
pub use dozer_api::grpc::internal_grpc::internal_pipeline_service_client;
use std::sync::{atomic::AtomicBool, Arc};

use dozer_types::{
    crossbeam::channel::Sender,
    models::{api_config::ApiConfig, api_endpoint::ApiEndpoint, source::Source},
};
use errors::OrchestrationError;
#[cfg(test)]
mod test_utils;
mod utils;

pub trait Orchestrator {
    fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self;
    fn add_endpoints(&mut self, endpoint: Vec<ApiEndpoint>) -> &mut Self;
    fn add_api_config(&mut self, api_config: ApiConfig) -> &mut Self;
    fn run_api(&mut self, running: Arc<AtomicBool>) -> Result<(), OrchestrationError>;
    fn run_apps(
        &mut self,
        running: Arc<AtomicBool>,
        api_notifier: Option<Sender<bool>>,
    ) -> Result<(), OrchestrationError>;
}

// Re-exports
use dozer_ingestion::connectors::TableInfo;
pub use dozer_ingestion::{connectors::get_connector, errors::ConnectorError};
pub use dozer_types::models::connection::Connection;
use dozer_types::tracing::error;

pub fn validate(input: Connection, tables: Vec<TableInfo>) -> Result<(), ConnectorError> {
    let connection_service = get_connector(input.clone())?;
    connection_service.validate(Some(tables)).map_err(|e| {
        error!(
            "Connection \"{}\" validation error: {:?}",
            input.name.clone(),
            e
        );
        e
    })
}
