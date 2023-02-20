use dozer_types::models::{
    api_config::{ApiConfig, GrpcApiOptions, RestApiOptions},
    api_security::ApiSecurity,
    app_config::Config,
};
use std::path::{Path, PathBuf};

pub fn get_pipeline_dir(config: &Config) -> PathBuf {
    AsRef::<Path>::as_ref(&config.home_dir).join("pipeline")
}
pub fn get_app_grpc_config(config: Config) -> GrpcApiOptions {
    config.api.unwrap_or_default().app_grpc.unwrap_or_default()
}
pub fn get_cache_dir(config: &Config) -> PathBuf {
    AsRef::<Path>::as_ref(&config.home_dir).join("cache")
}

pub fn get_max_map_size(config: &Config) -> usize {
    if let Some(max_map_size) = config.cache_max_map_size {
        max_map_size.try_into().unwrap()
    } else {
        1024 * 1024 * 1024 * 1024
    }
}

pub fn get_api_dir(config: &Config) -> PathBuf {
    AsRef::<Path>::as_ref(&config.home_dir).join("api")
}
pub fn get_grpc_config(config: Config) -> GrpcApiOptions {
    config.api.unwrap_or_default().grpc.unwrap_or_default()
}
pub fn get_api_config(config: Config) -> ApiConfig {
    config.api.unwrap_or_default()
}
pub fn get_rest_config(config: Config) -> RestApiOptions {
    config.api.unwrap_or_default().rest.unwrap_or_default()
}
pub fn get_api_security_config(config: Config) -> Option<ApiSecurity> {
    get_api_config(config).api_security
}

pub fn get_flags(config: Config) -> Option<dozer_types::models::flags::Flags> {
    config.flags
}

pub fn get_repl_history_path(config: &Config) -> PathBuf {
    AsRef::<Path>::as_ref(&config.home_dir).join("history.txt")
}
pub fn get_sql_history_path(config: &Config) -> PathBuf {
    AsRef::<Path>::as_ref(&config.home_dir).join("sql_history.txt")
}
