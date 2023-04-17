pub mod cli;
pub mod errors;
pub mod pipeline;
pub mod shutdown;
pub mod simple;

use dozer_core::{app::AppPipeline, errors::ExecutionError};
use dozer_ingestion::connectors::SourceSchema;
use dozer_sql::pipeline::{builder::statement_to_pipeline, errors::PipelineError};
use dozer_types::{crossbeam::channel::Sender, log::debug};
use errors::OrchestrationError;
use shutdown::{ShutdownReceiver, ShutdownSender};
use std::{
    backtrace::{Backtrace, BacktraceStatus},
    collections::HashMap,
    panic, process,
    thread::current,
};
use tokio::task::JoinHandle;
mod console_helper;
pub mod utils;

pub trait Orchestrator {
    fn migrate(&mut self, force: bool) -> Result<(), OrchestrationError>;
    fn clean(&mut self) -> Result<(), OrchestrationError>;
    fn deploy(&mut self, deploy: Deploy, config_path: String) -> Result<(), OrchestrationError>;
    fn run_all(&mut self, shutdown: ShutdownReceiver) -> Result<(), OrchestrationError>;
    fn run_api(&mut self, shutdown: ShutdownReceiver) -> Result<(), OrchestrationError>;
    fn run_apps(
        &mut self,
        shutdown: ShutdownReceiver,
        api_notifier: Option<Sender<bool>>,
    ) -> Result<(), OrchestrationError>;
    #[allow(clippy::type_complexity)]
    fn list_connectors(
        &self,
    ) -> Result<HashMap<String, (Vec<TableInfo>, Vec<SourceSchema>)>, OrchestrationError>;
    fn generate_token(&self) -> Result<String, OrchestrationError>;
}

// Re-exports
pub use dozer_ingestion::{
    connectors::{get_connector, TableInfo},
    errors::ConnectorError,
};
pub use dozer_sql::pipeline::builder::QueryContext;

pub fn wrapped_statement_to_pipeline(sql: &str) -> Result<QueryContext, PipelineError> {
    let mut pipeline = AppPipeline::new();
    statement_to_pipeline(sql, &mut pipeline, None)
}

use crate::cli::types::Deploy;
pub use dozer_types::models::connection::Connection;
use dozer_types::tracing::error;

async fn flatten_join_handle(
    handle: JoinHandle<Result<(), OrchestrationError>>,
) -> Result<(), OrchestrationError> {
    match handle.await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(OrchestrationError::InternalError(Box::new(err))),
    }
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

        let backtrace = Backtrace::capture();
        if backtrace.status() == BacktraceStatus::Captured {
            error!(
                "thread '{}' panicked at '{}'\n stack backtrace:\n{}",
                current()
                    .name()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                panic_info
                    .location()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                backtrace
            );
        }

        process::exit(1);
    }));
}

pub fn set_ctrl_handler(shutdown_sender: ShutdownSender) {
    let mut shutdown = Some(shutdown_sender);
    ctrlc::set_handler(move || {
        if let Some(shutdown) = shutdown.take() {
            shutdown.shutdown()
        }
    })
    .expect("Error setting Ctrl-C handler");
}
