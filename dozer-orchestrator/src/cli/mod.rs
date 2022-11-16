mod types;
use clap::Parser;
use dozer_orchestrator::errors::OrchestrationError;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::source::Source;
use serde::{Deserialize, Serialize};
use std::fs;
use types::{Cli, Commands};

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

pub fn cli_parse() {
    let cli = Cli::parse();

    let args = Cli::parse();

    match args.cmd {
        Commands::Api(apiCmd) => {}
        Commands::App(appCmd) => todo!(),
        Commands::Ps => todo!(),
    }

    if let Some(cmd) = args.cmd {
        match cmd {
            SubCommand::GenerateToken => generate_token(),
        }
    } else {
        run(args.config_path)
    }
}
