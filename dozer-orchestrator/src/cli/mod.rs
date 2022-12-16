#[cfg(test)]
pub mod tests;
pub mod types;
use super::OrchestrationError;
use dozer_types::{models::app_config::Config, serde_yaml};
use std::fs;
pub fn load_config(config_path: String) -> Result<Config, OrchestrationError> {
    let contents = fs::read_to_string(config_path).map_err(OrchestrationError::FailedToLoadFile)?;
    let config = serde_yaml::from_str(&contents)
        .map_err(|e| OrchestrationError::FailedToParseYaml(Box::new(e)))?;
    Ok(config)
}
