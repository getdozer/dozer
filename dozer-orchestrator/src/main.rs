mod cli;

use clap::Parser;

use log::debug;
use std::sync::Arc;

use crate::cli::{load_config, Args, SubCommand};
use dozer_orchestrator::simple::SimpleOrchestrator as Dozer;
use dozer_orchestrator::Orchestrator;
use dozer_schema::registry::_get_client;
use tokio::runtime::Runtime;

fn main() -> anyhow::Result<()> {
    log4rs::init_file("log4rs.yaml", Default::default())
        .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));

    debug!(
        "
      ____   ___ __________ ____
     |  _ \\ / _ \\__  / ____|  _ \\
     | | | | | | |/ /|  _| | |_) |
     | |_| | |_| / /_| |___|  _ <
     |____/ \\___/____|_____|_| \\_\\"
    );
    let args = Args::parse();

    match args.cmd {
        SubCommand::Run { config_name } => {
            let configuration = load_config(config_name);

            let client = Runtime::new()
                .unwrap()
                .block_on(async { _get_client().await.unwrap() });

            let mut dozer = Dozer::new(Arc::new(client));
            dozer.add_sources(configuration.sources);
            dozer.add_endpoints(configuration.endpoints);
            dozer.run()
        }
    }
}
