use clap::Parser;
#[cfg(feature = "cloud")]
use dozer_orchestrator::cli::cloud::CloudCommands;
use dozer_orchestrator::cli::generate_config_repl;
use dozer_orchestrator::cli::types::{ApiCommands, AppCommands, Cli, Commands, ConnectorCommands};
use dozer_orchestrator::cli::{init_dozer, list_sources, LOGO};
use dozer_orchestrator::errors::{CliError, OrchestrationError};
use dozer_orchestrator::simple::SimpleOrchestrator;
#[cfg(feature = "cloud")]
use dozer_orchestrator::CloudOrchestrator;
use dozer_orchestrator::{set_ctrl_handler, set_panic_hook, shutdown, Orchestrator};
use dozer_types::models::telemetry::TelemetryConfig;
use dozer_types::tracing::{error, info};
use futures::executor;
use serde::Deserialize;

use std::process;

fn main() {
    set_panic_hook();

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

#[derive(Deserialize, Debug)]
struct DozerPackage {
    pub latestVersion: String,
    pub availableAssets: Vec<String>,
    pub link: String,
}

async fn check_update() {
    use std::println as info;
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    const ARCH: &str = std::env::consts::ARCH;
    const OS: &str = std::env::consts::OS;

    info!("Checking for updates...");
    let request_url = format!(
        "https://metadata.dev.getdozer.io/?version={}&build={}&os={}",
        VERSION, ARCH, OS
    );
    info!("{}", request_url);
    let response = reqwest::get(&request_url).await;
    match response {
        Ok(r) => {
            let package: DozerPackage = r.json().await.unwrap();
            info!("A new version is available.");
            info!(
                "You can download Dozer v{}, from {}.",
                package.latestVersion, package.link
            );
        }
        Err(e) => {
            info!("Failed to check for updates: {}", e);
        }
    }
}

fn run() -> Result<(), OrchestrationError> {
    // Reloading trace layer seems impossible, so we are running Cli::parse in a closure
    // and then initializing it after reading the configuration. This is a hacky workaround, but it works.

    executor::block_on(check_update());

    let cli = parse_and_generate()?;
    let mut dozer = init_orchestrator(&cli)?;

    let (shutdown_sender, shutdown_receiver) = shutdown::new(&dozer.runtime);
    set_ctrl_handler(shutdown_sender);

    // Now we have access to telemetry configuration
    let _telemetry = Telemetry::new(Some(&dozer.config.app_name), dozer.config.telemetry.clone());

    if let Some(cmd) = cli.cmd {
        // run individual servers
        match cmd {
            Commands::Api(api) => match api.command {
                ApiCommands::Run => {
                    render_logo();

                    dozer.run_api(shutdown_receiver)
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

                    dozer.run_apps(shutdown_receiver, None)
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
            #[cfg(feature = "cloud")]
            Commands::Cloud(cloud) => match cloud.command.clone() {
                CloudCommands::Deploy => dozer.deploy(cloud, cli.config_path),
                CloudCommands::List => dozer.list(cloud),
                CloudCommands::Status(ref app) => dozer.status(cloud, app.app_id.clone()),
            },
            Commands::Init => {
                panic!("This should not happen as it is handled in parse_and_generate");
            }
        }
    } else {
        render_logo();

        dozer.run_all(shutdown_receiver)
    }
}

// Some commands dont need to initialize the orchestrator
// This function is used to run those commands
fn parse_and_generate() -> Result<Cli, OrchestrationError> {
    dozer_tracing::init_telemetry_closure(None, None, || -> Result<Cli, OrchestrationError> {
        let cli = Cli::parse();

        if let Some(Commands::Init) = cli.cmd {
            if let Err(e) = generate_config_repl() {
                error!("{}", e);
                Err(e)
            } else {
                // We need to exit here, otherwise the orchestrator will be initialized
                process::exit(0);
            }
        } else {
            Ok(cli)
        }
    })
}

fn init_orchestrator(cli: &Cli) -> Result<SimpleOrchestrator, CliError> {
    dozer_tracing::init_telemetry_closure(None, None, || -> Result<SimpleOrchestrator, CliError> {
        let res = init_dozer(cli.config_path.clone());

        match res {
            Ok(dozer) => Ok(dozer),
            Err(e) => {
                error!("{}", e);
                Err(e)
            }
        }
    })
}

struct Telemetry(dozer_tracing::WorkerGuard);

impl Telemetry {
    fn new(app_name: Option<&str>, config: Option<TelemetryConfig>) -> Self {
        Self(dozer_tracing::init_telemetry(app_name, config))
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        dozer_tracing::shutdown_telemetry();
    }
}
