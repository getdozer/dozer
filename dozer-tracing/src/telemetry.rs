use dozer_types::log::debug;
use dozer_types::models::telemetry::{DozerTelemetryConfig, OpenTelemetryConfig, TelemetryConfig};
use opentelemetry::sdk;
use opentelemetry::sdk::trace::{BatchConfig, BatchSpanProcessor, Sampler};
use opentelemetry::trace::TracerProvider;
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Layer};

use crate::exporter::DozerExporter;
// Init telemetry by setting a global handler
pub fn init_telemetry(
    app_name: Option<&str>,
    telemetry_config: Option<TelemetryConfig>,
) -> WorkerGuard {
    let app_name = app_name.unwrap_or("dozer");

    // disable errors from open telemetry
    opentelemetry::global::set_error_handler(|_| {}).unwrap();

    debug!("Initializing telemetry for {:?}", telemetry_config);

    let fmt_layer = fmt::layer().with_target(false);
    let fmt_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    let log_writer_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    let layers = telemetry_config.map_or((None, None), |c| {
        let trace_filter = EnvFilter::try_from_env("DOZER_TRACE_FILTER")
            .or_else(|_| EnvFilter::try_new("dozer=debug"))
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

    let file_appender = tracing_appender::rolling::never("./log", format!("{app_name}.log"));
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(fmt_layer.with_filter(fmt_filter))
        .with(
            fmt::Layer::default()
                .with_writer(non_blocking)
                .with_filter(log_writer_filter),
        )
        .with(layers.0)
        .with(layers.1)
        .init();

    guard
}

// Cleanly shutdown telemetry
pub fn shutdown_telemetry() {
    opentelemetry::global::shutdown_tracer_provider();
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

    let log_writer_filter = EnvFilter::try_from_default_env()
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

    let file_appender = tracing_appender::rolling::never("./log", format!("{app_name}.log"));
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let subscriber = tracing_subscriber::registry()
        .with(fmt::Layer::default().with_writer(non_blocking.clone()))
        .with(fmt_layer.with_filter(fmt_filter))
        .with(
            fmt::Layer::default()
                .with_writer(non_blocking)
                .with_filter(log_writer_filter),
        )
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
        .install_batch(opentelemetry::runtime::TokioCurrentThread)
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
    let sample_percent = config.sample_percent as f64 / 100.0;
    let exporter = DozerExporter::new(config);
    let batch_config = BatchConfig::default()
        .with_max_concurrent_exports(100000)
        .with_max_concurrent_exports(5);
    let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(sample_percent)));
    let batch_processor =
        BatchSpanProcessor::builder(exporter, opentelemetry::runtime::TokioCurrentThread)
            .with_batch_config(batch_config)
            .build();

    let tracer_provider = builder
        .with_config(opentelemetry::sdk::trace::Config {
            sampler: Box::new(sampler),
            ..Default::default()
        })
        .with_span_processor(batch_processor)
        .build();

    let tracer = tracer_provider.versioned_tracer(
        "opentelemetry-dozer",
        Some(env!("CARGO_PKG_VERSION")),
        None,
    );
    let _ = global::set_tracer_provider(tracer_provider);
    tracing_opentelemetry::layer().with_tracer(tracer)
}
