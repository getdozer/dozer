use std::time::Duration;

use dozer_types::log::{debug, error};
use dozer_types::models::telemetry::{
    DozerTelemetryConfig, TelemetryConfig, TelemetryTraceConfig, XRayConfig,
};
use dozer_types::tracing::{self, Metadata, Subscriber};
use metrics_exporter_prometheus::PrometheusBuilder;
use opentelemetry::global;
use opentelemetry::sdk::trace::{self, XrayIdGenerator};
use opentelemetry::sdk::trace::{BatchConfig, BatchSpanProcessor, Sampler};
use opentelemetry::sdk::{self, Resource};
use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{ExportConfig, WithExportConfig};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{filter, fmt, EnvFilter, Layer};

use crate::exporter::DozerExporter;
// Init telemetry by setting a global handler
pub fn init_telemetry(app_name: Option<&str>, telemetry_config: Option<TelemetryConfig>) {
    // log errors from open telemetry
    opentelemetry::global::set_error_handler(|e| {
        error!("OpenTelemetry error: {}", e);
    })
    .unwrap();

    debug!("Initializing telemetry for {:?}", telemetry_config);

    let subscriber = create_subscriber(app_name, telemetry_config.as_ref());
    subscriber.init();

    if let Some(telemetry_config) = telemetry_config {
        if telemetry_config.metrics.is_some() {
            PrometheusBuilder::new()
                .install()
                .expect("Failed to install Prometheus recorder/exporter");
        }
    }
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
    let subscriber = create_subscriber(app_name, telemetry_config.as_ref());

    dozer_types::tracing::subscriber::with_default(subscriber, closure)
}

fn create_subscriber(
    app_name: Option<&str>,
    telemetry_config: Option<&TelemetryConfig>,
) -> impl Subscriber {
    let app_name = app_name.unwrap_or("dozer");

    let fmt_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    let layers = telemetry_config.map_or((None, None), |c| {
        let trace_filter = EnvFilter::try_from_env("DOZER_TRACE_FILTER")
            .or_else(|_| EnvFilter::try_new("dozer=trace"))
            .unwrap();
        match &c.trace {
            None => (None, None),
            Some(TelemetryTraceConfig::Dozer(config)) => (
                Some(get_dozer_tracer(config).with_filter(trace_filter)),
                None,
            ),
            Some(TelemetryTraceConfig::XRay(config)) => (
                None,
                Some(
                    get_xray_tracer(app_name, config).with_filter(filter::filter_fn(
                        |metadata: &Metadata| metadata.level() == &tracing::Level::ERROR,
                    )),
                ),
            ),
        }
    });

    let stdout_is_tty = atty::is(atty::Stream::Stdout);
    tracing_subscriber::registry()
        .with(
            fmt::Layer::default()
                .without_time()
                .with_target(!stdout_is_tty)
                .with_ansi(stdout_is_tty)
                .with_filter(fmt_filter),
        )
        .with(layers.0)
        .with(layers.1)
}

fn get_xray_tracer<S>(
    app_name: &str,
    config: &XRayConfig,
) -> OpenTelemetryLayer<S, opentelemetry::sdk::trace::Tracer>
where
    S: for<'span> tracing_subscriber::registry::LookupSpan<'span>
        + dozer_types::tracing::Subscriber,
{
    let otlp_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_export_config(ExportConfig {
            endpoint: config.endpoint.clone(),
            protocol: opentelemetry_otlp::Protocol::Grpc,
            timeout: Duration::from_secs(config.timeout_in_seconds),
        })
        .with_timeout(Duration::from_secs(3));

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(otlp_exporter)
        .with_trace_config(
            trace::config()
                .with_id_generator(XrayIdGenerator::default())
                .with_resource(Resource::new(vec![KeyValue::new(
                    "service.name",
                    app_name.to_string(),
                )])),
        )
        .install_simple()
        .expect("Failed to install OpenTelemetry tracer.");
    tracing_opentelemetry::layer().with_tracer(tracer)
}

fn get_dozer_tracer<S>(
    config: &DozerTelemetryConfig,
) -> OpenTelemetryLayer<S, opentelemetry::sdk::trace::Tracer>
where
    S: for<'span> tracing_subscriber::registry::LookupSpan<'span>
        + dozer_types::tracing::Subscriber,
{
    let builder = sdk::trace::TracerProvider::builder();
    let sample_percent = config.sample_percent as f64 / 100.0;
    let exporter = DozerExporter::new(config.clone());
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
        None::<String>,
        None,
    );
    let _ = global::set_tracer_provider(tracer_provider);
    tracing_opentelemetry::layer().with_tracer(tracer)
}
