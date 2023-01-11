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
use tokio::task::JoinHandle;
mod console_helper;
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

pub use dozer_types::models::connection::Connection;
use dozer_types::tracing::error;
use dozer_types::types::Schema;

async fn flatten_joinhandle(
    handle: JoinHandle<Result<(), OrchestrationError>>,
) -> Result<(), OrchestrationError> {
    match handle.await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(OrchestrationError::InternalError(Box::new(err))),
    }
}

pub fn validate(input: Connection, tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
    get_connector(input)?.validate(tables)
}

pub fn validate_schema(
    input: Connection,
    tables: &Vec<TableInfo>,
) -> Result<HashMap<String, Vec<(Option<String>, Result<(), ConnectorError>)>>, ConnectorError> {
    get_connector(input)?.validate_schemas(tables)
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
