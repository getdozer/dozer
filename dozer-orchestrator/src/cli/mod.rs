#[cfg(test)]
pub mod tests;
pub mod types;
use crate::errors::CliError;
use dozer_types::{models::app_config::Config, serde_yaml};
use std::fs;
pub fn load_config(config_path: String) -> Result<Config, CliError> {
    let contents = fs::read_to_string(config_path.clone())
        .map_err(|_| CliError::FailedToLoadFile(config_path))?;
    let config =
        serde_yaml::from_str(&contents).map_err(|e| CliError::FailedToParseYaml(Box::new(e)))?;
    println!("==== config {:?}", config);
    Ok(config)
}

pub const LOGO: &str = "
____   ___ __________ ____
|  _ \\ / _ \\__  / ____|  _ \\
| | | | | | |/ /|  _| | |_) |
| |_| | |_| / /_| |___|  _ <
|____/ \\___/____|_____|_| \\_\\
";

pub const DESCRIPTION: &str = "Open-source platform to build, publish and manage blazing-fast real-time data APIs in minutes.";
