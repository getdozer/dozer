use dozer_types::log::debug;
use dozer_types::models::telemetry::{DozerTelemetryConfig, OpenTelemetryConfig, TelemetryConfig};
use opentelemetry::sdk;
use opentelemetry::trace::TracerProvider;
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Layer};

use crate::exporter::DozerExporter;
// Init telemetry by setting a global handler
pub fn init_telemetry(app_name: Option<&str>, telemetry_config: Option<TelemetryConfig>) {
    let app_name = app_name.unwrap_or("dozer");

    debug!("Initializing telemetry for {:?}", telemetry_config);

    let fmt_layer = fmt::layer().with_target(false);
    let fmt_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    let layers = telemetry_config.map_or((None, None), |c| {
        let trace_filter = EnvFilter::try_from_env("DOZER_TRACE_FILTER")
            .or_else(|_| EnvFilter::try_new("dozer=trace"))
            .unwrap();
        match c {
            TelemetryConfig::Dozer(config) => (
                Some(get_dozer_tracer(config).with_filter(trace_filter)),
                None,
            ),
            TelemetryConfig::OpenTelemetry(config) => (
                None,
                Some(get_otel_tracer(app_name, config).with_filter(trace_filter)),
            ),
        }
    });

    tracing_subscriber::registry()
        .with(fmt_layer.with_filter(fmt_filter))
        .with(layers.0)
        .with(layers.1)
        .init();
}

// Init telemetry with a closure without setting a global subscriber
pub fn init_telemetry_closure<T>(
    app_name: Option<&str>,
    telemetry_config: Option<TelemetryConfig>,
    closure: impl FnOnce() -> T,
) -> T {
    let app_name = app_name.unwrap_or("dozer");

    let fmt_layer = fmt::layer().with_target(false);
    let fmt_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    let layers = telemetry_config.map_or((None, None), |c| {
        let trace_filter = EnvFilter::try_from_env("DOZER_TRACE_FILTER")
            .or_else(|_| EnvFilter::try_new("dozer=trace"))
            .unwrap();
        match c {
            TelemetryConfig::Dozer(config) => (
                Some(get_dozer_tracer(config).with_filter(trace_filter)),
                None,
            ),
            TelemetryConfig::OpenTelemetry(config) => (
                None,
                Some(get_otel_tracer(app_name, config).with_filter(trace_filter)),
            ),
        }
    });

    let subscriber = tracing_subscriber::registry()
        .with(fmt_layer.with_filter(fmt_filter))
        .with(layers.0)
        .with(layers.1);

    dozer_types::tracing::subscriber::with_default(subscriber, closure)
}

pub fn get_otel_tracer<S>(
    app_name: &str,
    _config: OpenTelemetryConfig,
) -> OpenTelemetryLayer<S, opentelemetry::sdk::trace::Tracer>
where
    S: for<'span> tracing_subscriber::registry::LookupSpan<'span>
        + dozer_types::tracing::Subscriber,
{
    global::set_text_map_propagator(TraceContextPropagator::new());
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name(app_name)
        // .install_batch(opentelemetry::runtime::TokioCurrentThread)
        .install_simple()
        .expect("Failed to install OpenTelemetry tracer.");
    tracing_opentelemetry::layer().with_tracer(tracer)
}

pub fn get_dozer_tracer<S>(
    config: DozerTelemetryConfig,
) -> OpenTelemetryLayer<S, opentelemetry::sdk::trace::Tracer>
where
    S: for<'span> tracing_subscriber::registry::LookupSpan<'span>
        + dozer_types::tracing::Subscriber,
{
    let builder = sdk::trace::TracerProvider::builder();
    let tracer_provider = builder
        .with_simple_exporter(DozerExporter::new(config))
        .build();

    let tracer = tracer_provider.versioned_tracer(
        "opentelemetry-dozer",
        Some(env!("CARGO_PKG_VERSION")),
        None,
    );
    let _ = global::set_tracer_provider(tracer_provider);
    tracing_opentelemetry::layer().with_tracer(tracer)
}
