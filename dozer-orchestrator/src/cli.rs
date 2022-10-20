use clap::{Parser, Subcommand};
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::source::Source;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Parser, Debug)]
#[command(author, version)]
pub struct Args {
    #[clap(subcommand)]
    pub cmd: SubCommand,
}

#[derive(Subcommand, Debug)]
pub enum SubCommand {
    // Todo: implement tables support
    #[command(
        author,
        version,
        about = "Run orchestration",
        long_about = "Run orchestration"
    )]
    Run {
        #[arg(short = 'c', long, default_value = "./dozer-config.yaml")]
        config_path: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub storage_path: Option<String>,
    pub sources: Vec<Source>,
    pub endpoints: Vec<ApiEndpoint>,
}

pub fn load_config(config_path: String) -> Config {
    let contents = fs::read_to_string(config_path)
        .expect("Should have been able to read the file");

    serde_yaml::from_str(&contents).unwrap()
}
