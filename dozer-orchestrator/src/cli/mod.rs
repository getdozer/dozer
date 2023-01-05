#[cfg(test)]
pub mod tests;
pub mod types;
use crate::errors::CliError;
use dozer_types::{models::app_config::Config, serde_yaml};
use handlebars::Handlebars;
use std::{collections::BTreeMap, fs};

pub fn load_config(config_path: String) -> Result<Config, CliError> {
    let contents = fs::read_to_string(config_path.clone())
        .map_err(|_| CliError::FailedToLoadFile(config_path))?;

    let mut handlebars = Handlebars::new();
    handlebars
        .register_template_string("config", contents)
        .map_err(|e| CliError::FailedToParseYaml(Box::new(e)))?;

    let mut data = BTreeMap::new();

    for (key, value) in std::env::vars() {
        data.insert(key, value);
    }

    let config_str = handlebars
        .render("config", &data)
        .map_err(|e| CliError::FailedToParseYaml(Box::new(e)))?;

    let config =
        serde_yaml::from_str(&config_str).map_err(|e| CliError::FailedToParseYaml(Box::new(e)))?;
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
