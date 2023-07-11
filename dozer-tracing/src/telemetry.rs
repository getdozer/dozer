use crate::exporter::DozerExporter;
use crate::{
    API_LATENCY_HISTOGRAM_NAME, DATA_LATENCY_HISTOGRAM_NAME, PIPELINE_LATENCY_HISTOGRAM_NAME,
};
use dozer_types::log::{debug, error};
use dozer_types::models::telemetry::{
    DozerTelemetryConfig, JaegerTelemetryConfig, TelemetryConfig, TelemetryTraceConfig,
};
use dozer_types::tracing::Subscriber;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use opentelemetry::sdk;
use opentelemetry::sdk::trace::{BatchConfig, BatchSpanProcessor, Sampler};
use opentelemetry::trace::TracerProvider;
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Layer};
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
            let mut prom_builder = PrometheusBuilder::new();
            prom_builder = prom_builder
                .set_buckets_for_metric(
                    Matcher::Full(API_LATENCY_HISTOGRAM_NAME.to_owned()),
                    &[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0],
                )
                .unwrap();
            prom_builder = prom_builder
                .set_buckets_for_metric(
                    Matcher::Full(DATA_LATENCY_HISTOGRAM_NAME.to_owned()),
                    &[
                        0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.00, 5.0, 10.0, 30.0,
                        40.0, 50.0, 60.0,
                    ],
                )
                .unwrap();
            prom_builder = prom_builder
                .set_buckets_for_metric(
                    Matcher::Full(PIPELINE_LATENCY_HISTOGRAM_NAME.to_owned()),
                    &[
                        0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.00, 5.0, 10.0, 30.0,
                        40.0, 50.0, 60.0,
                    ],
                )
                .unwrap();
            prom_builder
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
            Some(TelemetryTraceConfig::Jaeger(config)) => (
                None,
                Some(get_jaeger_tracer(app_name, config).with_filter(trace_filter)),
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

fn get_jaeger_tracer<S>(
    app_name: &str,
    _config: &JaegerTelemetryConfig,
) -> OpenTelemetryLayer<S, opentelemetry::sdk::trace::Tracer>
where
    S: for<'span> tracing_subscriber::registry::LookupSpan<'span>
        + dozer_types::tracing::Subscriber,
{
    global::set_text_map_propagator(TraceContextPropagator::new());
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name(app_name)
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
        None,
    );
    let _ = global::set_tracer_provider(tracer_provider);
    tracing_opentelemetry::layer().with_tracer(tracer)
}
