mod cli;

use clap::Parser;

use dozer_types::errors::orchestrator::OrchestrationError;
use log::warn;
use std::sync::Arc;
use std::{thread, time};

use crate::cli::{load_config, Args, SubCommand};
use dozer_orchestrator::simple::SimpleOrchestrator as Dozer;
use dozer_orchestrator::Orchestrator;
use dozer_schema::registry::{_get_client, _serve};
use tokio::runtime::Runtime;

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
    let args = Args::parse();
    match args.cmd {
        SubCommand::Run { config_path } => {
            let configuration = load_config(config_path);

            let _thread = thread::spawn(|| {
                Runtime::new().unwrap().block_on(async {
                    tokio::spawn(_serve(None)).await.unwrap().unwrap();
                });
            });

            let ten_millis = time::Duration::from_millis(100);
            thread::sleep(ten_millis);

            let client = Runtime::new()
                .unwrap()
                .block_on(async { _get_client().await.unwrap() });

            let mut dozer = Dozer::new(Arc::new(client));
            dozer.add_sources(configuration.sources);
            dozer.add_endpoints(configuration.endpoints);
            dozer.run()?;
            Ok(())
        }
    }
}
