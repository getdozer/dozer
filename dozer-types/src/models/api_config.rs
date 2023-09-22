use super::{api_security::ApiSecurity, equal_default};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The security configuration for the API; Default: None
    pub api_security: Option<ApiSecurity>,

    #[serde(default, skip_serializing_if = "equal_default")]
    pub rest: RestApiOptions,

    #[serde(default, skip_serializing_if = "equal_default")]
    pub grpc: GrpcApiOptions,

    #[serde(default, skip_serializing_if = "equal_default")]
    pub app_grpc: AppGrpcOptions,

    #[serde(skip_serializing_if = "Option::is_none")]
    // max records to be returned from the endpoints
    pub default_max_num_records: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct RestApiOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cors: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct GrpcApiOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cors: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub web: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct AppGrpcOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

pub fn default_app_grpc_port() -> u32 {
    50053
}

pub fn default_app_grpc_host() -> String {
    "0.0.0.0".to_owned()
}

pub fn default_grpc_port() -> u16 {
    50051
}

pub fn default_rest_port() -> u16 {
    8080
}

pub fn default_host() -> String {
    "0.0.0.0".to_owned()
}
