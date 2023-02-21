use dozer_types::models::{
    api_config::{ApiConfig, GrpcApiOptions, RestApiOptions},
    api_security::ApiSecurity,
    app_config::Config,
};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

pub fn get_pipeline_dir(config: &Config) -> PathBuf {
    AsRef::<Path>::as_ref(&config.home_dir).join("pipeline")
}
pub fn get_app_grpc_config(config: Config) -> GrpcApiOptions {
    config.api.unwrap_or_default().app_grpc.unwrap_or_default()
}
pub fn get_cache_dir(config: &Config) -> PathBuf {
    AsRef::<Path>::as_ref(&config.home_dir).join("cache")
}

pub fn get_cache_max_map_size(config: &Config) -> usize {
    if let Some(max_map_size) = config.cache_max_map_size {
        max_map_size.try_into().unwrap()
    } else {
        1024 * 1024 * 1024 * 1024
    }
}

pub fn get_app_max_map_size(config: &Config) -> usize {
    if let Some(max_map_size) = config.app_max_map_size {
        max_map_size.try_into().unwrap()
    } else {
        1024 * 1024 * 1024 * 1024
    }
}

pub fn get_commit_time_threshold(config: &Config) -> std::time::Duration {
    if let Some(commit_time_threshold) = config.commit_timeout {
        Duration::from_millis(commit_time_threshold)
    } else {
        Duration::from_millis(50)
    }
}

pub fn get_buffer_size(config: &Config) -> usize {
    if let Some(app_buffer_size) = config.app_buffer_size {
        app_buffer_size as usize
    } else {
        20_000
    }
}

pub fn get_commit_size(config: &Config) -> u32 {
    if let Some(commit_size) = config.commit_size {
        commit_size
    } else {
        10_000
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
