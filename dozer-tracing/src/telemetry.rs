use std::time::Duration;

use dozer_types::log::{debug, error};
use dozer_types::models::telemetry::{
    TelemetryConfig, TelemetryMetricsConfig, TelemetryTraceConfig, XRayConfig,
};
use dozer_types::tracing::{self, Metadata, Subscriber};
use opentelemetry::metrics::noop::NoopMeterProvider;
use opentelemetry::{global, KeyValue};
use opentelemetry_aws::trace::XrayIdGenerator;
use opentelemetry_otlp::{ExportConfig, WithExportConfig};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::{self};
use opentelemetry_sdk::{self, Resource};
use prometheus::Registry;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{filter, fmt, EnvFilter, Layer};

use crate::prometheus_server::serve;
use crate::TracingError;

// Init telemetry by setting a global handler
pub struct Telemetry {
    registry: Option<Registry>,
    config: TelemetryConfig,
}

impl Telemetry {
    pub fn new(app_name: Option<&str>, config: &TelemetryConfig) -> Self {
        let registry = init_telemetry(app_name, config);
        Self {
            registry,
            config: config.clone(),
        }
    }

    pub async fn serve(&self) -> Result<(), TracingError> {
        if let Some(TelemetryMetricsConfig::Prometheus(config)) = &self.config.metrics {
            let registry = self
                .registry
                .as_ref()
                .expect("Prometheus Registry is expected");
            serve(registry, config.address.as_str()).await.unwrap();
            Ok(())
        } else {
            Err(TracingError::NotPrometheus)
        }
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        shutdown_telemetry();
    }
}

pub fn init_telemetry(
    app_name: Option<&str>,
    telemetry_config: &TelemetryConfig,
) -> Option<Registry> {
    // log errors from open telemetry
    global::set_error_handler(|e| {
        error!("OpenTelemetry error: {}", e);
    })
    .unwrap();

    debug!("Initializing telemetry for {:?}", telemetry_config);

    let subscriber = create_subscriber(app_name, telemetry_config, true);
    subscriber.init();

    if telemetry_config.metrics.is_some() {
        match init_metrics_provider() {
            Ok(r) => Some(r),
            Err(_) => None,
        }
    } else {
        global::set_meter_provider(NoopMeterProvider::new());
        None
    }
}

// Cleanly shutdown telemetry
pub fn shutdown_telemetry() {
    global::shutdown_tracer_provider();

    // shutdown
    global::set_meter_provider(NoopMeterProvider::new())
}

pub fn init_metrics_provider() -> Result<Registry, Box<dyn std::error::Error + Send + Sync>> {
    let registry = Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()?;
    let provider = SdkMeterProvider::builder()
        .with_reader(exporter)
        .with_resource(Resource::new(vec![KeyValue::new(
            "dozer.metrics",
            "replication",
        )]))
        .build();
    opentelemetry::global::set_meter_provider(provider);

    Ok(registry)
}

// Init telemetry with a closure without setting a global subscriber
pub fn init_telemetry_closure<T>(
    app_name: Option<&str>,
    telemetry_config: &TelemetryConfig,
    closure: impl FnOnce() -> T,
) -> T {
    let subscriber = create_subscriber(app_name, telemetry_config, false);

    dozer_types::tracing::subscriber::with_default(subscriber, closure)
}

fn create_subscriber(
    app_name: Option<&str>,
    telemetry_config: &TelemetryConfig,
    init_console_subscriber: bool,
) -> impl Subscriber {
    let app_name = app_name.unwrap_or("dozer");

    let fmt_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info,clickhouse_rs=error"))
        .unwrap();

    // `console_subscriber` can only be added once.
    #[cfg(feature = "tokio-console")]
    let console_layer = if init_console_subscriber {
        Some(console_subscriber::spawn())
    } else {
        None
    };
    #[cfg(not(feature = "tokio-console"))]
    let _ = init_console_subscriber;

    let layers = telemetry_config
        .trace
        .as_ref()
        .map(|TelemetryTraceConfig::XRay(config)| {
            get_xray_tracer(app_name, config).with_filter(filter::filter_fn(
                |metadata: &Metadata| metadata.level() == &tracing::Level::ERROR,
            ))
        });

    let stdout_is_tty = atty::is(atty::Stream::Stdout);
    let subscriber = tracing_subscriber::registry();
    #[cfg(feature = "tokio-console")]
    let subscriber = subscriber.with(console_layer);
    subscriber
        .with(
            fmt::Layer::default()
                .without_time()
                .with_target(!stdout_is_tty)
                .with_ansi(stdout_is_tty)
                .with_filter(fmt_filter),
        )
        .with(layers)
}

fn get_xray_tracer<S>(
    app_name: &str,
    config: &XRayConfig,
) -> OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>
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
