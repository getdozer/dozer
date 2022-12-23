pub mod cli;
pub mod errors;
pub mod internal;
pub mod pipeline;
pub mod simple;
pub use dozer_api::grpc::internal_grpc;
pub use dozer_api::grpc::internal_grpc::internal_pipeline_service_client;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};

use dozer_types::{crossbeam::channel::Sender, types::Schema};
use errors::OrchestrationError;
#[cfg(test)]
mod test_utils;
mod utils;

pub trait Orchestrator {
    fn init(&mut self) -> Result<(), OrchestrationError>;
    fn run_api(&mut self, running: Arc<AtomicBool>) -> Result<(), OrchestrationError>;
    fn run_apps(
        &mut self,
        running: Arc<AtomicBool>,
        api_notifier: Option<Sender<bool>>,
    ) -> Result<(), OrchestrationError>;
    fn list_connectors(&self)
        -> Result<HashMap<String, Vec<(String, Schema)>>, OrchestrationError>;
}

// Re-exports
pub use dozer_ingestion::{connectors::get_connector, errors::ConnectorError};
pub use dozer_types::models::connection::Connection;

pub fn validate(input: Connection) -> Result<(), ConnectorError> {
    let connection_service = get_connector(input)?;
    connection_service.validate()
}
