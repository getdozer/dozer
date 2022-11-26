use dozer_types::models::connection::{Authentication};
use dozer_types::models::source::Source;
use dozer_types::serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Deserialize, Serialize)]
pub struct DebeziumTestConfig {
    pub source: Source,
    pub postgres_source_authentication: Authentication,
    pub debezium_connector_url: String,
}

pub fn load_config(config_path: String) -> Result<DebeziumTestConfig, serde_yaml::Error> {
    let contents = fs::read_to_string(config_path).unwrap();

    serde_yaml::from_str::<DebeziumTestConfig>(&contents)
}
