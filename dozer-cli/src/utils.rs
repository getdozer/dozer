use dozer_core::executor::ExecutorOptions;
use dozer_types::models::{
    app_config::{default_app_buffer_size, default_error_threshold, default_event_hub_capacity},
    config::Config,
};

fn get_buffer_size(config: &Config) -> u32 {
    config
        .app
        .app_buffer_size
        .unwrap_or_else(default_app_buffer_size)
}

fn get_error_threshold(config: &Config) -> u32 {
    config
        .app
        .error_threshold
        .unwrap_or_else(default_error_threshold)
}

fn get_event_hub_capacity(config: &Config) -> usize {
    config
        .app
        .event_hub_capacity
        .unwrap_or_else(default_event_hub_capacity)
}

pub fn get_executor_options(config: &Config) -> ExecutorOptions {
    ExecutorOptions {
        channel_buffer_sz: get_buffer_size(config) as usize,
        error_threshold: Some(get_error_threshold(config)),
        event_hub_capacity: get_event_hub_capacity(config),
    }
}
