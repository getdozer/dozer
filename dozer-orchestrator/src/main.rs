use clap::Parser;
use dozer_orchestrator::cli::generate_config_repl;
use dozer_orchestrator::cli::types::{ApiCommands, AppCommands, Cli, Commands, ConnectorCommands};
use dozer_orchestrator::cli::{configure, init_dozer, list_sources, LOGO};
use dozer_orchestrator::errors::{CliError, OrchestrationError};
use dozer_orchestrator::simple::SimpleOrchestrator;
use dozer_orchestrator::{set_ctrl_handler, set_panic_hook, Orchestrator};

use dozer_types::log::{error, info};

use std::process;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

fn main() {
    if let Err(e) = run() {
        error!("{}", e);
        process::exit(1);
    }
}

fn render_logo() {
    use std::println as info;
    const VERSION: &str = env!("CARGO_PKG_VERSION");

    info!("{LOGO}");
    info!("\nDozer Version: {VERSION}\n");
}

fn run() -> Result<(), OrchestrationError> {
    set_panic_hook();

    // Reloading trace layer seems impossible, so we are running Cli::parse in a closure
    // and then initializing it after reading the configuration. This is a hacky workaround, but it works.

    let res = dozer_tracing::init_telemetry_closure(
        None,
        None,
        || -> Result<(Cli, SimpleOrchestrator), CliError> {
            let cli = Cli::parse();
            let dozer = init_dozer(cli.config_path.clone())?;

            Ok((cli, dozer))
        },
    );

    let (cli, mut dozer) = match res {
        Ok((cli, dozer)) => (cli, dozer),
        Err(e) => {
            error!("{}", e);
            process::exit(1);
        }
    };

    // Now we have acces to telemetry configuration
    let telemetry_config = dozer.config.telemetry.clone();

    dozer_tracing::init_telemetry(None, telemetry_config);

    let running = Arc::new(AtomicBool::new(true));
    set_ctrl_handler(running.clone());
    if let Some(cmd) = cli.cmd {
        // run individual servers
        match cmd {
            Commands::Api(api) => match api.command {
                ApiCommands::Run => {
                    render_logo();

                    dozer.run_api(running)
                }
                ApiCommands::GenerateToken => {
                    let token = dozer.generate_token()?;
                    info!("token: {:?} ", token);
                    Ok(())
                }
            },
            Commands::App(apps) => match apps.command {
                AppCommands::Run => {
                    render_logo();

                    dozer.run_apps(running, None)
                }
            },
            Commands::Connector(sources) => match sources.command {
                ConnectorCommands::Ls => list_sources(&cli.config_path),
            },
            Commands::Migrate(migrate) => {
                let force = migrate.force.is_some();

                dozer.migrate(force)
            }
            Commands::Clean => dozer.clean(),
            Commands::Configure => configure(cli.config_path, running),
            Commands::Init => generate_config_repl(),
        }
    } else {
        render_logo();

        let mut dozer = init_dozer(cli.config_path)?;
        dozer.run_all(running)
    }
}
