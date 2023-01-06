use clap::Parser;
use dozer_core::dag::errors::ExecutionError;
use dozer_orchestrator::cli::types::{ApiCommands, AppCommands, Cli, Commands, ConnectorCommands};
use dozer_orchestrator::cli::{configure, init_dozer, list_sources, LOGO};
use dozer_orchestrator::errors::OrchestrationError;
use dozer_orchestrator::{ConnectorError, Orchestrator};
use dozer_types::crossbeam::channel;
use dozer_types::log::{error, info};
use dozer_types::tracing::warn;
use std::borrow::BorrowMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{panic, process, thread};
use tokio::runtime::Runtime;

fn main() {
    if let Err(e) = run() {
        error!("{}", e);
        process::exit(1);
    }
}

fn render_logo() {
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    info!("Dozer Version: {}", VERSION);
    info!("{}", LOGO);
}
fn set_panic_hook() {
    panic::set_hook(Box::new(move |panic_info| {
        // All the orchestrator errors are captured here
        if let Some(e) = panic_info.payload().downcast_ref::<OrchestrationError>() {
            error!("{}", e);
        // All the connector errors are captured here
        } else if let Some(e) = panic_info.payload().downcast_ref::<ConnectorError>() {
            error!("{}", e);
        // All the pipeline errors are captured here
        } else if let Some(e) = panic_info.payload().downcast_ref::<ExecutionError>() {
            error!("{}", e);
        // If any errors are sent as strings.
        } else if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            error!("{s:?}");
        } else {
            error!("{}", panic_info);
        }
        process::exit(1);
    }));
}
fn run() -> Result<(), OrchestrationError> {
    let _tracing_thread = thread::spawn(|| {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            dozer_tracing::init_telemetry(false).unwrap();
        });
    });
    thread::sleep(Duration::from_millis(50));

    set_panic_hook();
    let cli = Cli::parse();
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let running_api = running.clone();

    // run all
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    if let Some(cmd) = cli.cmd {
        // run individual servers
        match cmd {
            Commands::Api(api) => match api.command {
                ApiCommands::Run => {
                    render_logo();
                    let mut dozer = init_dozer(cli.config_path)?;
                    dozer.run_api(running)
                }
                ApiCommands::GenerateToken => {
                    let dozer = init_dozer(cli.config_path)?;
                    let token = dozer.generate_token()?;
                    info!("token: {:?} ", token);
                    Ok(())
                }
            },
            Commands::App(apps) => match apps.command {
                AppCommands::Run => {
                    render_logo();
                    let mut dozer = init_dozer(cli.config_path)?;
                    dozer.run_apps(running, None)
                }
            },
            Commands::Connector(sources) => match sources.command {
                ConnectorCommands::Ls => list_sources(cli.config_path),
            },
            Commands::Init(init) => {
                let force = init.force.is_some();
                let mut dozer = init_dozer(cli.config_path)?;
                dozer.init(force)
            }
            Commands::Clean => {
                let mut dozer = init_dozer(cli.config_path)?;
                dozer.clean()
            }
            Commands::Configure => configure(cli.config_path, running),
        }
    } else {
        render_logo();
        let mut dozer = init_dozer(cli.config_path)?;
        let mut dozer_api = dozer.clone();

        let (tx, rx) = channel::unbounded::<bool>();

        if let Err(e) = dozer.init(false) {
            if let OrchestrationError::InitializationFailed(_) = e {
                warn!(
                    "{} is already present. Skipping initialisation..",
                    dozer.config.home_dir.to_owned()
                )
            } else {
                return Err(e);
            }
        }

        let pipeline_thread = thread::spawn(move || {
            if let Err(e) = dozer.borrow_mut().run_apps(running, Some(tx)) {
                std::panic::panic_any(e);
            }
        });

        // Wait for pipeline to initialize caches before starting api server
        rx.recv().unwrap();

        thread::spawn(move || {
            if let Err(e) = dozer_api.run_api(running_api) {
                std::panic::panic_any(e);
            }
        });

        pipeline_thread.join().unwrap();
        Ok(())
    }
}
