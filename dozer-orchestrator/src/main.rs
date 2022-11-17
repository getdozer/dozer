mod cli;
use clap::Parser;

use cli::load_config;
use cli::types::{ApiCommands, AppCommands, Cli, Commands};
use dozer_orchestrator::errors::OrchestrationError;
use dozer_orchestrator::simple::SimpleOrchestrator as Dozer;
use dozer_orchestrator::Orchestrator;
use log::warn;
use std::path::Path;

fn main() -> Result<(), OrchestrationError> {
    log4rs::init_file("log4rs.yaml", Default::default())
        .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));

    warn!(
        "
      ____   ___ __________ ____
     |  _ \\ / _ \\__  / ____|  _ \\
     | | | | | | |/ /|  _| | |_) |
     | |_| | |_| / /_| |___|  _ <
     |____/ \\___/____|_____|_| \\_\\"
    );

    let cli = Cli::parse();

    let configuration = load_config(cli.config_path)?;
    let path = Path::new("./dozer").to_owned();
    let mut dozer = Dozer::new(path);
    dozer.add_sources(configuration.sources);
    dozer.add_endpoints(configuration.endpoints);

    match cli.cmd {
        Commands::Api(api) => match api.command {
            ApiCommands::Run => dozer.run_api(),
            ApiCommands::GenerateToken => todo!(),
        },
        Commands::App(apps) => match apps.command {
            AppCommands::Run => dozer.run_apps(),
        },
        Commands::Ps => todo!(),
    }
}
