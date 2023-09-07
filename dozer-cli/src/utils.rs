use dozer_cache::cache::CacheManagerOptions;
use dozer_core::{
    checkpoint::CheckpointFactoryOptions, epoch::EpochManagerOptions, executor::ExecutorOptions,
};
use dozer_types::models::{
    api_config::{
        default_api_grpc, default_api_rest, default_app_grpc, AppGrpcOptions, GrpcApiOptions,
        RestApiOptions,
    },
    api_security::ApiSecurity,
    app_config::{
        default_app_buffer_size, default_commit_size, default_commit_timeout,
        default_error_threshold, default_max_interval_before_persist_in_seconds,
        default_max_num_records_before_persist, default_persist_queue_capacity, DataStorage,
    },
    config::{default_cache_max_map_size, Config},
};
use std::time::Duration;

fn get_cache_max_map_size(config: &Config) -> u64 {
    config
        .cache_max_map_size
        .unwrap_or_else(default_cache_max_map_size)
}

fn get_commit_time_threshold(config: &Config) -> Duration {
    Duration::from_millis(
        config
            .app
            .as_ref()
            .and_then(|app| app.commit_timeout)
            .unwrap_or_else(default_commit_timeout),
    )
}

fn get_buffer_size(config: &Config) -> u32 {
    config
        .app
        .as_ref()
        .and_then(|app| app.app_buffer_size)
        .unwrap_or_else(default_app_buffer_size)
}

fn get_commit_size(config: &Config) -> u32 {
    config
        .app
        .as_ref()
        .and_then(|app| app.commit_size)
        .unwrap_or_else(default_commit_size)
}

fn get_error_threshold(config: &Config) -> u32 {
    config
        .app
        .as_ref()
        .and_then(|app| app.error_threshold)
        .unwrap_or_else(default_error_threshold)
}

fn get_max_num_records_before_persist(config: &Config) -> usize {
    config
        .app
        .as_ref()
        .and_then(|app| app.max_num_records_before_persist)
        .unwrap_or_else(default_max_num_records_before_persist) as usize
}

fn get_max_interval_before_persist_in_seconds(config: &Config) -> u64 {
    config
        .app
        .as_ref()
        .and_then(|app| app.max_interval_before_persist_in_seconds)
        .unwrap_or_else(default_max_interval_before_persist_in_seconds)
}

pub fn get_storage_config(config: &Config) -> DataStorage {
    let app = config.app.as_ref();
    app.and_then(|app| app.data_storage.clone())
        .unwrap_or_default()
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

pub fn get_checkpoint_factory_options(config: &Config) -> CheckpointFactoryOptions {
    CheckpointFactoryOptions {
        persist_queue_capacity: config
            .app
            .as_ref()
            .and_then(|app| app.persist_queue_capacity)
            .unwrap_or_else(default_persist_queue_capacity)
            as usize,
        storage_config: get_storage_config(config),
    }
}

pub fn get_executor_options(config: &Config) -> ExecutorOptions {
    ExecutorOptions {
        commit_sz: get_commit_size(config),
        channel_buffer_sz: get_buffer_size(config) as usize,
        commit_time_threshold: get_commit_time_threshold(config),
        error_threshold: Some(get_error_threshold(config)),
        epoch_manager_options: EpochManagerOptions {
            max_num_records_before_persist: get_max_num_records_before_persist(config),
            max_interval_before_persist_in_seconds: get_max_interval_before_persist_in_seconds(
                config,
            ),
        },
    }
}

pub fn get_cache_manager_options(config: &Config) -> CacheManagerOptions {
    CacheManagerOptions {
        path: Some(config.cache_dir.clone().into()),
        max_size: get_cache_max_map_size(config) as usize,
        ..CacheManagerOptions::default()
    }
}
