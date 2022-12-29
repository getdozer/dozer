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
    pub pipeline_internal: Option<ApiPipelineInternal>,
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
    #[serde(default = "default_rest_port")]
    pub port: u32,
    #[prost(string, tag = "2")]
    #[serde(default = "default_host")]
    pub host: String,
    #[prost(bool, tag = "3")]
    #[serde(default = "default_cors")]
    pub cors: bool,
}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
pub struct ApiGrpc {
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
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
pub struct ApiPipelineInternal {
    #[prost(uint32, tag = "1")]
    #[serde(default = "default_pipeline_internal_port")]
    pub port: u32,
    #[prost(string, tag = "2")]
    #[serde(default = "default_pipeline_internal_host")]
    pub host: String,
    #[prost(string, tag = "3")]
    #[serde(default = "default_pipeline_internal_home_dir")]
    pub home_dir: String,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
pub struct ApiInternal {
    #[prost(string, tag = "1")]
    #[serde(default = "default_api_internal_home_dir")]
    pub home_dir: String,
}

fn default_api_internal_home_dir() -> String {
    format!("{:}/api", DEFAULT_HOME_DIR.to_owned())
}
fn default_api_internal() -> Option<ApiInternal> {
    Some(ApiInternal {
        home_dir: format!("{:}/api", DEFAULT_HOME_DIR.to_owned()),
    })
}
fn default_pipeline_internal_port() -> u32 {
    50053
}
fn default_pipeline_internal_host() -> String {
    "[::1]".to_owned()
}
fn default_pipeline_internal_home_dir() -> String {
    format!("{:}/pipeline", DEFAULT_HOME_DIR.to_owned())
}
pub(crate) fn default_pipeline_internal() -> Option<ApiPipelineInternal> {
    Some(ApiPipelineInternal {
        port: default_pipeline_internal_port(),
        host: default_pipeline_internal_host(),
        home_dir: default_pipeline_internal_home_dir(),
    })
}
pub(crate) fn default_api_rest() -> Option<ApiRest> {
    Some(ApiRest {
        port: default_rest_port(),
        host: default_host(),
        cors: default_cors(),
    })
}
pub(crate) fn default_api_grpc() -> Option<ApiGrpc> {
    Some(ApiGrpc {
        port: default_grpc_port(),
        host: default_host(),
        cors: default_cors(),
        web: default_enable_web(),
    })
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

fn default_host() -> String {
    "[::0]".to_owned()
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
