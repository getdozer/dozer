use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    pub trace: Option<TelemetryTraceConfig>,
    pub metrics: Option<TelemetryMetricsConfig>,
    #[serde(default)]
    pub application_id: usize,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Clone)]
pub enum TelemetryTraceConfig {
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

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub enum TelemetryMetricsConfig {
    Prometheus(PrometheusConfig),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct PrometheusConfig {
    #[serde(default = "PrometheusConfig::default_address")]
    pub address: String,
}
impl PrometheusConfig {
    pub fn default_address() -> String {
        "0.0.0.0:8089".to_string()
    }
}
