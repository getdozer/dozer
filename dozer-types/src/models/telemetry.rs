use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    pub trace: Option<TelemetryTraceConfig>,
    pub metrics: Option<TelemetryMetricsConfig>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Clone)]
pub enum TelemetryTraceConfig {
    Dozer(DozerTelemetryConfig),
    XRay(XRayConfig),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct DozerTelemetryConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub adapter: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_percent: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct XRayConfig {
    pub endpoint: String,
    pub timeout_in_seconds: u64,
}

pub fn default_ingest_address() -> String {
    "0.0.0.0:7006".to_string()
}

pub fn default_sample_ratio() -> u32 {
    10
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub enum TelemetryMetricsConfig {
    Prometheus,
}
