use std::fs;

use crate::errors::AdminError;
use dozer_types::models::api_config::{default_api_config, ApiInternal};
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::serde_yaml;
pub mod cli_process;
pub mod types;
pub mod utils;
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct AdminCliConfig {
    pub port: u32,
    pub host: String,
    pub cors: bool,
    #[serde(default = "GrpcInternal::default")]
    pub internal: GrpcInternal,
    pub dozer_config: Option<String>,
    #[serde(default = "default_output_path")]
    pub output_path: String,
}
fn default_output_path() -> String {
    "./.dozer".to_owned()
}
impl Default for AdminCliConfig {
    fn default() -> Self {
        Self {
            port: 8081,
            host: "[::0]".to_owned(),
            cors: true,
            internal: GrpcInternal::default(),
            dozer_config: None,
            output_path: default_output_path(),
        }
    }
}
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct GrpcInternal {
    pub api: ApiInternal,
    pub pipeline: ApiInternal,
}
impl Default for GrpcInternal {
    fn default() -> Self {
        let default_config = default_api_config();
        Self {
            api: default_config.api_internal.unwrap(),
            pipeline: default_config.pipeline_internal.unwrap(),
        }
    }
}

pub fn load_config(config_path: String) -> Result<AdminCliConfig, AdminError> {
    let contents = fs::read_to_string(config_path).map_err(AdminError::FailedToLoadFile)?;
    let config: AdminCliConfig =
        serde_yaml::from_str(&contents).map_err(|e| AdminError::FailedToParseYaml(Box::new(e)))?;
    Ok(config)
}
