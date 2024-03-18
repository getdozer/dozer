mod telemetry;
pub use telemetry::{init_telemetry, init_telemetry_closure, shutdown_telemetry};
mod context;
pub use context::DozerMonitorContext;
pub mod metrics;
mod prometheus_server;

pub use opentelemetry::{global, metrics as opentelemetry_metrics, KeyValue};
pub use telemetry::Telemetry;

#[derive(dozer_types::thiserror::Error, Debug)]
pub enum TracingError {
    #[error("Metrics is not enabled")]
    NotPrometheus,
}
