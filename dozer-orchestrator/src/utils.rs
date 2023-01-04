use dozer_types::models::{
    api_config::{ApiConfig, ApiGrpc, ApiPipelineInternal, ApiRest},
    api_security::ApiSecurity,
    app_config::Config,
};
use std::path::PathBuf;

pub fn get_pipeline_dir(config: Config) -> PathBuf {
    PathBuf::from(format!("{:}/pipeline", config.home_dir))
}
pub fn get_pipeline_config(config: Config) -> ApiPipelineInternal {
    config
        .api
        .unwrap_or_default()
        .pipeline_internal
        .unwrap_or_default()
}
pub fn get_cache_dir(config: Config) -> PathBuf {
    PathBuf::from(format!("{:}/cache", config.home_dir))
}

pub fn get_api_dir(config: Config) -> PathBuf {
    PathBuf::from(format!("{:}/api", config.home_dir))
}
pub fn get_grpc_config(config: Config) -> ApiGrpc {
    config.api.unwrap_or_default().grpc.unwrap_or_default()
}
pub fn get_api_config(config: Config) -> ApiConfig {
    config.api.unwrap_or_default()
}
pub fn get_rest_config(config: Config) -> ApiRest {
    config.api.unwrap_or_default().rest.unwrap_or_default()
}
pub fn get_api_security_config(config: Config) -> Option<ApiSecurity> {
    get_api_config(config).api_security
}
