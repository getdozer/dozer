use serde::{Deserialize, Serialize};

use crate::constants::DEFAULT_HOME_DIR;

use super::api_security::ApiSecurity;
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
#[serde(default = "default_api_config")]
pub struct ApiConfig {
    #[prost(oneof = "ApiSecurity", tags = "1")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_security: Option<ApiSecurity>,
    #[prost(message, tag = "2")]
    #[serde(default = "default_api_rest")]
    pub rest: Option<ApiRest>,
    #[prost(message, tag = "3")]
    #[serde(default = "default_api_grpc")]
    pub grpc: Option<ApiGrpc>,
    #[prost(bool, tag = "4")]
    pub auth: bool,
    #[prost(message, tag = "5")]
    #[serde(default = "default_api_internal")]
    pub api_internal: Option<ApiInternal>,
    #[prost(message, tag = "6")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default = "default_pipeline_internal")]
    pub pipeline_internal: Option<ApiInternal>,
    #[prost(string, optional, tag = "7")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[prost(string, optional, tag = "8")]
    pub id: Option<String>,
}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
pub struct ApiRest {
    #[prost(uint32, tag = "1")]
    pub port: u32,
    #[prost(string, tag = "2")]
    pub url: String,
    #[prost(bool, tag = "3")]
    pub cors: bool,
}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
pub struct ApiGrpc {
    #[prost(uint32, tag = "1")]
    pub port: u32,
    #[prost(string, tag = "2")]
    pub url: String,
    #[prost(bool, tag = "3")]
    pub cors: bool,
    #[prost(bool, tag = "4")]
    pub web: bool,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
pub struct ApiInternal {
    #[prost(uint32, tag = "1")]
    pub port: u32,
    #[prost(string, tag = "2")]
    pub host: String,
    #[prost(string, tag = "3")]
    pub home_dir: String,
}

fn default_api_internal() -> Option<ApiInternal> {
    Some(ApiInternal {
        port: 50052,
        host: "[::1]".to_owned(),
        home_dir: format!("{:}/api", DEFAULT_HOME_DIR.to_owned()),
    })
}
fn default_pipeline_internal() -> Option<ApiInternal> {
    Some(ApiInternal {
        port: 50053,
        host: "[::1]".to_owned(),
        home_dir: format!("{:}/pipeline", DEFAULT_HOME_DIR.to_owned()),
    })
}
fn default_api_rest() -> Option<ApiRest> {
    Some(ApiRest {
        port: 8080,
        url: "[::0]".to_owned(),
        cors: true,
    })
}
fn default_api_grpc() -> Option<ApiGrpc> {
    Some(ApiGrpc {
        port: 50051,
        url: "[::0]".to_owned(),
        cors: true,
        web: true,
    })
}
pub fn default_api_config() -> ApiConfig {
    ApiConfig {
        rest: default_api_rest(),
        grpc: default_api_grpc(),
        auth: false,
        api_internal: default_api_internal(),
        pipeline_internal: default_pipeline_internal(),
        ..Default::default()
    }
}
