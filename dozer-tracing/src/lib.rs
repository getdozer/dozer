mod metrics_recorder;

use metrics_recorder::MetricsRecorder;
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};
use tempdir::TempDir;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

pub fn init_metrics() {
    let path = TempDir::new("dozer_metrics")
        .expect("unable to create temp dir for metrics")
        .path()
        .to_owned();
    let recorder = MetricsRecorder::new(path);
    metrics::set_boxed_recorder(Box::new(recorder)).unwrap()
}

pub fn init_telemetry() -> Result<(), Box<dyn ::std::error::Error>> {
    let app_name = "dozer";

    global::set_text_map_propagator(TraceContextPropagator::new());
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name(app_name)
        // .install_batch(opentelemetry::runtime::TokioCurrentThread)
        .install_simple()
        .expect("Failed to install OpenTelemetry tracer.");

    let fmt_layer = fmt::layer().with_target(false);
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .with(telemetry)
        .init();

    Ok(())
}
