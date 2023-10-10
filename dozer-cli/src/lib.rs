pub mod cli;
pub mod errors;
pub mod live;
pub mod pipeline;
pub mod simple;
use dozer_api::shutdown::ShutdownSender;
use dozer_core::{app::AppPipeline, errors::ExecutionError};
use dozer_sql::{builder::statement_to_pipeline, errors::PipelineError};
use dozer_types::log::debug;
use errors::OrchestrationError;
use futures::Future;
use std::{
    backtrace::{Backtrace, BacktraceStatus},
    panic, process,
    thread::current,
};
use tokio::task::{JoinError, JoinHandle};
pub mod cloud;
pub mod config_helper;
pub use dozer_api::shutdown;
pub mod console_helper;
#[cfg(test)]
mod tests;
mod utils;
// Re-exports
pub use dozer_ingestion::{
    connectors::{get_connector, TableInfo},
    errors::ConnectorError,
};
pub use dozer_sql::builder::QueryContext;
pub fn wrapped_statement_to_pipeline(sql: &str) -> Result<QueryContext, PipelineError> {
    let mut pipeline = AppPipeline::new_with_default_flags();
    statement_to_pipeline(sql, &mut pipeline, None, vec![])
}

pub use dozer_types::models::connection::Connection;
use dozer_types::tracing::error;

async fn flatten_join_handle(
    handle: JoinHandle<Result<(), OrchestrationError>>,
) -> Result<(), OrchestrationError> {
    match handle.await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(OrchestrationError::JoinError(err)),
    }
}

fn join_handle_map_err<E: Send + 'static>(
    handle: JoinHandle<Result<(), E>>,
    f: impl FnOnce(E) -> OrchestrationError + Send + 'static,
) -> JoinHandle<Result<(), OrchestrationError>> {
    tokio::spawn(async move {
        match handle.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(err)) => Err(f(err)),
            Err(err) => Err(OrchestrationError::JoinError(err)),
        }
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

pub async fn set_ctrl_handler(shutdown_sender: ShutdownSender) {
    tokio::spawn(async {
        tokio::signal::ctrl_c()
            .await
            .expect("Error setting Ctrl-C handler");
        shutdown_sender.shutdown();
    });
}
