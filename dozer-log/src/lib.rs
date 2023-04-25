pub mod errors;
pub mod reader;
pub mod schemas;

use std::path::{Path, PathBuf};

pub use tokio;

fn get_logs_path(pipeline_dir: &Path) -> PathBuf {
    pipeline_dir.join("logs")
}

fn get_endpoint_log_dir(pipeline_dir: &Path, endpoint_name: &str) -> PathBuf {
    get_logs_path(pipeline_dir).join(endpoint_name.to_lowercase())
}

pub fn get_endpoint_log_path(pipeline_dir: &Path, endpoint_name: &str) -> PathBuf {
    get_endpoint_log_dir(pipeline_dir, endpoint_name).join("log")
}

pub fn get_endpoint_schema_path(pipeline_dir: &Path, endpoint_name: &str) -> PathBuf {
    get_endpoint_log_dir(pipeline_dir, endpoint_name).join("schema.json")
}
