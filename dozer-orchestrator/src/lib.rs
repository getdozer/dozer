pub mod cli;
pub mod errors;
pub mod pipeline;
pub mod simple;
pub use dozer_api::grpc::internal_grpc;
pub use dozer_api::grpc::internal_grpc::internal_pipeline_service_client;
use dozer_core::dag::errors::ExecutionError;
use dozer_types::{
    crossbeam::channel::Sender,
    log::debug,
    types::{Operation, SchemaWithChangesType},
};
use errors::OrchestrationError;
use std::{
    collections::HashMap,
    panic, process,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
#[cfg(test)]
mod test_utils;
mod utils;

pub trait Orchestrator {
    fn init(&mut self, force: bool) -> Result<(), OrchestrationError>;
    fn clean(&mut self) -> Result<(), OrchestrationError>;
    fn run_api(&mut self, running: Arc<AtomicBool>) -> Result<(), OrchestrationError>;
    fn run_apps(
        &mut self,
        running: Arc<AtomicBool>,
        api_notifier: Option<Sender<bool>>,
    ) -> Result<(), OrchestrationError>;
    fn list_connectors(
        &self,
    ) -> Result<HashMap<String, Vec<SchemaWithChangesType>>, OrchestrationError>;
    fn generate_token(&self) -> Result<String, OrchestrationError>;
    fn query(
        &self,
        sql: String,
        sender: Sender<Operation>,
        running: Arc<AtomicBool>,
    ) -> Result<Schema, OrchestrationError>;
}

// Re-exports
use dozer_ingestion::connectors::TableInfo;
pub use dozer_ingestion::{connectors::get_connector, errors::ConnectorError};
use dozer_types::log::info;
pub use dozer_types::models::connection::Connection;
use dozer_types::tracing::error;
use dozer_types::types::Schema;

pub fn validate(input: Connection, tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
    let connection_service = get_connector(input.clone())?;
    connection_service.validate(tables).map(|t| {
        info!("[{}] Validation completed", input.name);
        t
    })
}

pub fn set_panic_hook() {
    panic::set_hook(Box::new(move |panic_info| {
        // All the orchestrator errors are captured here
        if let Some(e) = panic_info.payload().downcast_ref::<OrchestrationError>() {
            error!("{}", e);
            debug!("{:?}", e);
        // All the connector errors are captured here
        } else if let Some(e) = panic_info.payload().downcast_ref::<ConnectorError>() {
            error!("{}", e);
            debug!("{:?}", e);
        // All the pipeline errors are captured here
        } else if let Some(e) = panic_info.payload().downcast_ref::<ExecutionError>() {
            error!("{}", e);
            debug!("{:?}", e);
        // If any errors are sent as strings.
        } else if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            error!("{s:?}");
        } else {
            error!("{}", panic_info);
        }

        process::exit(1);
    }));
}

pub fn set_ctrl_handler(r: Arc<AtomicBool>) {
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");
}
