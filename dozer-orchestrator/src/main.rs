mod cli;
use clap::Parser;

use cli::load_config;
use cli::types::{ApiCommands, AppCommands, Cli, Commands};
use dozer_orchestrator::errors::OrchestrationError;
use dozer_orchestrator::simple::SimpleOrchestrator as Dozer;
use dozer_orchestrator::Orchestrator;
use dozer_types::crossbeam::channel;
use log::{debug, warn};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

fn load_default_config() {
    let config_str = include_str!("../../config/log4rs.release.yaml");
    let config = serde_yaml::from_str(config_str).unwrap();
    log4rs::init_raw_config(config).unwrap();
}

fn main() -> Result<(), OrchestrationError> {
    log4rs::init_file("log4rs.yaml", Default::default()).unwrap_or_else(|_e| {
        debug!("log4rs.yaml not found. Loading default config.");
        load_default_config();
    });

    warn!(
        "
      ____   ___ __________ ____
     |  _ \\ / _ \\__  / ____|  _ \\
     | | | | | | |/ /|  _| | |_) |
     | |_| | |_| / /_| |___|  _ <
     |____/ \\___/____|_____|_| \\_\\"
    );

    let cli = Cli::parse();
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let running_api = running.clone();

    // run all
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let configuration = load_config(cli.config_path)?;
    let path = Path::new("./.dozer").to_owned();
    let mut dozer = Dozer::new(path);
    dozer.add_sources(configuration.sources);
    dozer.add_endpoints(configuration.endpoints);
    dozer.add_api_config(configuration.api);
    if let Some(cmd) = cli.cmd {
        // run individual servers
        match cmd {
            Commands::Api(api) => match api.command {
                ApiCommands::Run => dozer.run_api(running),
                ApiCommands::GenerateToken => todo!(),
            },
            Commands::App(apps) => match apps.command {
                AppCommands::Run => dozer.run_apps(running, None),
            },
            Commands::Ps => todo!(),
        }
    } else {
        let mut dozer_api = dozer.clone();

        let (tx, rx) = channel::unbounded::<bool>();
        thread::spawn(move || dozer.run_apps(running, Some(tx)));

        // Wait for pipeline to initialize caches before starting api server
        rx.recv().unwrap();

        dozer_api.run_api(running_api)
    }
}
