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
    #[prost(string, tag = "1", default = "0.0.0.0")]
    #[serde(default = "default_ingest_host")]
    pub host: String,
    #[prost(uint32, tag = "2", default = "7006")]
    #[serde(default = "default_ingest_port")]
    pub port: u32,
    #[prost(string, tag = "5", default = "default")]
    #[serde(default = "default_grpc_adapter")]
    pub adapter: String,
}
fn default_grpc_adapter() -> String {
    "arrow".to_owned()
}

fn default_ingest_host() -> String {
    "0.0.0.0".to_owned()
}

fn default_ingest_port() -> u32 {
    7006
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
