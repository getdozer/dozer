use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, prost::Oneof)]

pub enum TelemetryConfig {
    #[prost(message, tag = "1")]
    Dozer(DozerTelemetryConfig),
    #[prost(message, tag = "2")]
    OpenTelemetry(OpenTelemetryConfig),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, prost::Message)]

pub struct DozerTelemetryConfig {
    #[prost(string, tag = "1", default = "0.0.0.0:7006")]
    #[serde(default = "default_ingest_address")]
    pub endpoint: String,
    #[prost(string, tag = "2", default = "default")]
    #[serde(default = "default_grpc_adapter")]
    pub adapter: String,
    #[prost(uint32, tag = "3")]
    #[serde(default = "default_sample_ratio")]
    pub sample_percent: u32,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, prost::Message)]

pub struct OpenTelemetryConfig {
    #[prost(string, tag = "1", default = "127.0.0.1:6831")]
    #[serde(default = "default_open_telemetry_endpoint")]
    pub endpoint: String,
}

fn default_open_telemetry_endpoint() -> String {
    "127.0.0.1:6831".to_string()
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
