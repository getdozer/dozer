use clap::{Parser, Subcommand};
use dozer_orchestrator::errors::OrchestrationError;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::source::Source;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Parser, Debug)]
#[command(author, version)]
pub struct Args {
    #[arg(short = 'c', long, default_value = "./dozer-config.yaml")]
    pub config_path: String,

    #[clap(subcommand)]
    pub cmd: Option<SubCommand>,
}

#[derive(Subcommand, Debug)]
pub enum SubCommand {
    // Todo: implement tables support
    #[command(
        author,
        version,
        about = "Generate master token",
        long_about = "Runs dozer orchestrator which brings up all the modules"
    )]
    GenerateToken,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub storage_path: Option<String>,
    pub sources: Vec<Source>,
    pub endpoints: Vec<ApiEndpoint>,
}

pub fn load_config(config_path: String) -> Result<Config, OrchestrationError> {
    let contents = fs::read_to_string(config_path).map_err(OrchestrationError::FailedToLoadFile)?;

    serde_yaml::from_str(&contents).map_err(|e| OrchestrationError::FailedToParseYaml(Box::new(e)))
}
