use dozer_cache::cache::CacheManagerOptions;
use dozer_core::executor::ExecutorOptions;
use dozer_types::models::{
    api_config::{
        default_api_grpc, default_api_rest, default_app_grpc, AppGrpcOptions, GrpcApiOptions,
        RestApiOptions,
    },
    api_security::ApiSecurity,
    app_config::{
        default_app_buffer_size, default_cache_max_map_size, default_commit_size,
        default_commit_timeout, default_log_entry_max_size, Config,
    },
};
use std::time::Duration;

fn get_cache_max_map_size(config: &Config) -> u64 {
    config
        .cache_max_map_size
        .unwrap_or_else(default_cache_max_map_size)
}

fn get_commit_time_threshold(config: &Config) -> Duration {
    if let Some(commit_time_threshold) = config.commit_timeout {
        Duration::from_millis(commit_time_threshold)
    } else {
        Duration::from_millis(default_commit_timeout())
    }
}

fn get_buffer_size(config: &Config) -> u32 {
    config
        .app_buffer_size
        .unwrap_or_else(default_app_buffer_size)
}

fn get_commit_size(config: &Config) -> u32 {
    config.commit_size.unwrap_or_else(default_commit_size)
}

pub fn get_log_entry_max_size(config: &Config) -> usize {
    config
        .log_entry_max_size
        .unwrap_or_else(default_log_entry_max_size) as usize
}

pub fn get_grpc_config(config: &Config) -> GrpcApiOptions {
    config
        .api
        .as_ref()
        .and_then(|api| api.grpc.clone())
        .unwrap_or_else(default_api_grpc)
}

pub fn get_rest_config(config: &Config) -> RestApiOptions {
    config
        .api
        .as_ref()
        .and_then(|api| api.rest.clone())
        .unwrap_or_else(default_api_rest)
}

pub fn get_app_grpc_config(config: &Config) -> AppGrpcOptions {
    config
        .api
        .as_ref()
        .and_then(|api| api.app_grpc.clone())
        .unwrap_or_else(default_app_grpc)
}

pub fn get_api_security_config(config: &Config) -> Option<&ApiSecurity> {
    config
        .api
        .as_ref()
        .and_then(|api| api.api_security.as_ref())
}

pub fn get_executor_options(config: &Config, err_threshold: Option<u32>) -> ExecutorOptions {
    ExecutorOptions {
        commit_sz: get_commit_size(config),
        channel_buffer_sz: get_buffer_size(config) as usize,
        commit_time_threshold: get_commit_time_threshold(config),
        error_threshold: Some(err_threshold.unwrap_or(0_u32)),
    }
}

pub fn get_cache_manager_options(config: &Config) -> CacheManagerOptions {
    CacheManagerOptions {
        path: Some(config.cache_dir.clone().into()),
        max_size: get_cache_max_map_size(config) as usize,
        ..CacheManagerOptions::default()
    }
}
