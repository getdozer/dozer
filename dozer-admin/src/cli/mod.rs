use std::fs;

use crate::errors::AdminError;
use dozer_types::models::api_config::ApiInternal;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::serde_yaml;
pub mod types;
pub mod utils;
pub mod cli_process;
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct AdminCliConfig {
    pub port: u32,
    pub host: String,
    pub cors: bool,
    pub internal: Option<GrpcInternal>,
    pub dozer_config: Option<String>,
}
impl Default for AdminCliConfig {
    fn default() -> Self {
        Self {
            port: 8081,
            host: "[::0]".to_owned(),
            cors: true,
            internal: Some(GrpcInternal::default()),
            dozer_config: None,
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
        Self {
            api: ApiInternal {
                port: 50052,
                host: "[::1]".to_owned(),
            },
            pipeline: ApiInternal {
                port: 50053,
                host: "[::1]".to_owned(),
            },
        }
    }
}

pub fn load_config(config_path: String) -> Result<AdminCliConfig, AdminError> {
    let contents = fs::read_to_string(config_path).map_err(AdminError::FailedToLoadFile)?;
    let config: AdminCliConfig =
        serde_yaml::from_str(&contents).map_err(|e| AdminError::FailedToParseYaml(Box::new(e)))?;
    Ok(config)
}
