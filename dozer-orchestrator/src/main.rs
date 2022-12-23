use clap::Parser;
use dozer_orchestrator::cli::types::{ApiCommands, AppCommands, Cli, Commands, ConnectorCommands};
use dozer_orchestrator::cli::{load_config, LOGO};
use dozer_orchestrator::errors::OrchestrationError;
use dozer_orchestrator::simple::SimpleOrchestrator as Dozer;
use dozer_orchestrator::Orchestrator;
use dozer_types::crossbeam::channel;
use dozer_types::log::info;
use dozer_types::tracing::debug;
use log::error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{panic, process, thread};
use tokio::runtime::Runtime;

fn main() {
    if let Err(e) = run() {
        debug!("{:?}", e);
        error!("{}", e);
    }
}

fn render_logo() {
    info!("{}", LOGO);
}

fn run() -> Result<(), OrchestrationError> {
    let _tracing_thread = thread::spawn(|| {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            dozer_tracing::init_telemetry(false).unwrap();
        });
    });
    thread::sleep(Duration::from_millis(50));

    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        process::exit(1);
    }));

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
    let mut dozer = Dozer::new(configuration);

    if let Some(cmd) = cli.cmd {
        // run individual servers
        match cmd {
            Commands::Api(api) => match api.command {
                ApiCommands::Run => {
                    render_logo();
                    dozer.run_api(running)
                }
                ApiCommands::GenerateToken => todo!(),
            },
            Commands::App(apps) => match apps.command {
                AppCommands::Run => {
                    render_logo();
                    dozer.run_apps(running, None)
                }
            },
            Commands::Connector(sources) => match sources.command {
                ConnectorCommands::Ls => {
                    let connection_map = dozer.list_connectors()?;
                    for (c, tables) in connection_map {
                        info!("------------Connection: {} ------------", c);
                        info!("");
                        for (schema_name, schema) in tables {
                            info!("Schema: {}", schema_name);

                            for f in schema.fields {
                                info!("  {}    -       {:?}", f.name, f.typ);
                            }
                            info!("");
                        }
                    }
                    Ok(())
                }
            },
        }
    } else {
        render_logo();

        let mut dozer_api = dozer.clone();

        let (tx, rx) = channel::unbounded::<bool>();

        let pipeline_thread = thread::spawn(move || {
            if let Err(e) = dozer.run_apps(running, Some(tx)) {
                panic!("Dozer Pipeline: {}", e.to_string());
            }
        });

        // Wait for pipeline to initialize caches before starting api server
        rx.recv().unwrap();

        thread::spawn(move || {
            if let Err(e) = dozer_api.run_api(running_api) {
                panic!("Dozer Api: {}", e.to_string());
            }
        });

        pipeline_thread.join().unwrap();
        Ok(())
    }
}
