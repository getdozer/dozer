pub mod types;
use dozer_orchestrator::errors::OrchestrationError;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::source::Source;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub sources: Vec<Source>,
    pub endpoints: Vec<ApiEndpoint>,
}

pub fn load_config(config_path: String) -> Result<Config, OrchestrationError> {
    let contents = fs::read_to_string(config_path).map_err(OrchestrationError::FailedToLoadFile)?;

    serde_yaml::from_str(&contents).map_err(|e| OrchestrationError::FailedToParseYaml(Box::new(e)))
}
