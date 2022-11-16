mod cli;

use clap::Parser;
use dozer_orchestrator::errors::OrchestrationError;
use log::warn;

use crate::cli::{cli_parse, load_config};
use dozer_orchestrator::simple::SimpleOrchestrator as Dozer;
use dozer_orchestrator::Orchestrator;

fn run(config_path: String) -> Result<(), OrchestrationError> {
    let configuration = load_config(config_path)?;
    let mut dozer = Dozer::new();
    dozer.add_sources(configuration.sources);
    dozer.add_endpoints(configuration.endpoints);
    dozer.run()
}
fn generate_token() -> Result<(), OrchestrationError> {
    todo!()
}
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

    Ok(())
}
