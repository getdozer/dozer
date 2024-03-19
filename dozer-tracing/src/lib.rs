mod telemetry;
use constants::ConnectorEntityType;
pub use telemetry::{init_telemetry, init_telemetry_closure, shutdown_telemetry};
mod context;
pub use context::DozerMonitorContext;
pub mod constants;
mod prometheus_server;

pub use opentelemetry::{global, metrics as opentelemetry_metrics, KeyValue};
pub use telemetry::Telemetry;
pub use tracing_opentelemetry;
pub use tracing_subscriber;

use crate::constants::CONNECTOR_EVENTS;

#[derive(dozer_types::thiserror::Error, Debug)]
pub enum TracingError {
    #[error("Metrics is not enabled")]
    NotPrometheus,
}

pub fn emit_event(
    entity_name: &str,
    entity_type: &ConnectorEntityType,
    labels: &DozerMonitorContext,
    event_name: &str,
) {
    let span = dozer_types::tracing::trace_span!(
        CONNECTOR_EVENTS,
        entity_name = entity_name,
        entity_type = entity_type.to_string(),
        application_id = labels.application_id,
        company_id = labels.company_id
    );

    span.in_scope(|| {
        dozer_types::tracing::event!(dozer_types::tracing::Level::TRACE, event_name = event_name);
    });
    drop(span);
}
