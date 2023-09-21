use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, PartialEq, Eq, Clone)]
pub struct TelemetryConfig {
    pub trace: Option<TelemetryTraceConfig>,

    pub metrics: Option<TelemetryMetricsConfig>,
}

#[derive(Debug, Serialize, JsonSchema, Deserialize, PartialEq, Eq, Clone)]
pub enum TelemetryTraceConfig {
    Dozer(DozerTelemetryConfig),

    XRay(XRayConfig),
}

#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, PartialEq, Eq, Clone)]
pub struct DozerTelemetryConfig {
    #[serde(default = "default_ingest_address")]
    pub endpoint: String,

    #[serde(default = "default_grpc_adapter")]
    pub adapter: String,

    #[serde(default = "default_sample_ratio")]
    pub sample_percent: u32,
}

#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, PartialEq, Eq, Clone)]
pub struct XRayConfig {
    pub endpoint: String,

    pub timeout_in_seconds: u64,
}

fn default_grpc_adapter() -> String {
    "arrow".to_owned()
}

fn default_ingest_address() -> String {
    "0.0.0.0:7006".to_string()
}

fn default_sample_ratio() -> u32 {
    10
}

#[derive(Debug, Serialize, JsonSchema, Deserialize, PartialEq, Eq, Clone)]
pub enum TelemetryMetricsConfig {
    Prometheus(()),
}
