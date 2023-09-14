use crate::constants::DEFAULT_DEFAULT_MAX_NUM_RECORDS;

use super::api_security::ApiSecurity;
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
pub struct ApiConfig {
    #[prost(oneof = "ApiSecurity", tags = "1")]
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The security configuration for the API; Default: None
    pub api_security: Option<ApiSecurity>,
    #[prost(message, tag = "2")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rest: Option<RestApiOptions>,
    #[prost(message, tag = "3")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc: Option<GrpcApiOptions>,

    #[prost(message, tag = "4")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_grpc: Option<AppGrpcOptions>,

    #[prost(uint32, tag = "5")]
    #[serde(default = "default_default_max_num_records")]
    // max records to be returned from the endpoints
    pub default_max_num_records: u32,
}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
pub struct RestApiOptions {
    #[prost(uint32, tag = "1")]
    #[serde(default = "default_rest_port")]
    pub port: u32,
    #[prost(string, tag = "2")]
    #[serde(default = "default_host")]
    pub host: String,
    #[prost(bool, tag = "3")]
    #[serde(default = "default_cors")]
    pub cors: bool,
    #[prost(bool, tag = "4")]
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
pub struct GrpcApiOptions {
    #[prost(uint32, tag = "1")]
    #[serde(default = "default_grpc_port")]
    pub port: u32,
    #[prost(string, tag = "2")]
    #[serde(default = "default_host")]
    pub host: String,
    #[prost(bool, tag = "3")]
    #[serde(default = "default_cors")]
    pub cors: bool,
    #[prost(bool, tag = "4")]
    #[serde(default = "default_enable_web")]
    pub web: bool,
    #[prost(bool, tag = "5")]
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message, Hash)]
pub struct AppGrpcOptions {
    #[prost(uint32)]
    #[serde(default = "default_app_grpc_port")]
    pub port: u32,
    #[prost(string)]
    #[serde(default = "default_app_grpc_host")]
    pub host: String,
}

fn default_app_grpc_port() -> u32 {
    50053
}
fn default_app_grpc_host() -> String {
    "0.0.0.0".to_owned()
}
pub fn default_default_max_num_records() -> u32 {
    DEFAULT_DEFAULT_MAX_NUM_RECORDS as u32
}

pub fn default_app_grpc() -> AppGrpcOptions {
    AppGrpcOptions {
        port: default_app_grpc_port(),
        host: default_app_grpc_host(),
    }
}
pub fn default_api_rest() -> RestApiOptions {
    RestApiOptions {
        port: default_rest_port(),
        host: default_host(),
        cors: default_cors(),
        enabled: true,
    }
}
pub fn default_api_grpc() -> GrpcApiOptions {
    GrpcApiOptions {
        port: default_grpc_port(),
        host: default_host(),
        cors: default_cors(),
        web: default_enable_web(),
        enabled: true,
    }
}
fn default_grpc_port() -> u32 {
    50051
}
fn default_rest_port() -> u32 {
    8080
}
fn default_enable_web() -> bool {
    true
}
fn default_cors() -> bool {
    true
}
fn default_enabled() -> bool {
    true
}

fn default_host() -> String {
    "0.0.0.0".to_owned()
}
