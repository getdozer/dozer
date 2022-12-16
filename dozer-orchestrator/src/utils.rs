use dozer_types::models::{
    api_config::{ApiGrpc, ApiRest},
    app_config::Config,
};
use std::path::PathBuf;

pub fn get_pipeline_dir(config: Config) -> PathBuf {
    PathBuf::from(
        config
            .api
            .unwrap_or_default()
            .pipeline_internal
            .unwrap_or_default()
            .home_dir,
    )
}

pub fn get_api_dir(config: Config) -> PathBuf {
    PathBuf::from(
        config
            .api
            .unwrap_or_default()
            .api_internal
            .unwrap_or_default()
            .home_dir,
    )
}
pub fn get_grpc_config(config: Config) -> ApiGrpc {
    config.api.unwrap_or_default().grpc.unwrap_or_default()
}

pub fn get_rest_config(config: Config) -> ApiRest {
    config.api.unwrap_or_default().rest.unwrap_or_default()
}
