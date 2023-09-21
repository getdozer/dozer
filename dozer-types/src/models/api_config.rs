use crate::constants::DEFAULT_DEFAULT_MAX_NUM_RECORDS;

use super::api_security::ApiSecurity;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, JsonSchema, Default)]
pub struct ApiConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The security configuration for the API; Default: None
    pub api_security: Option<ApiSecurity>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub rest: Option<RestApiOptions>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc: Option<GrpcApiOptions>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_grpc: Option<AppGrpcOptions>,

    #[serde(default = "default_default_max_num_records")]
    // max records to be returned from the endpoints
    pub default_max_num_records: u32,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, JsonSchema, Default)]
pub struct RestApiOptions {
    #[serde(default = "default_rest_port")]
    pub port: u32,

    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_cors")]
    pub cors: bool,

    #[serde(default = "default_enabled")]
    pub enabled: bool,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, JsonSchema, Default)]
pub struct GrpcApiOptions {
    #[serde(default = "default_grpc_port")]
    pub port: u32,

    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_cors")]
    pub cors: bool,

    #[serde(default = "default_enable_web")]
    pub web: bool,

    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema, Default)]
pub struct AppGrpcOptions {
    #[serde(default = "default_app_grpc_port")]
    pub port: u32,

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
