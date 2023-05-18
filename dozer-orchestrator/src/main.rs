use clap::Parser;
#[cfg(feature = "cloud")]
use dozer_orchestrator::cli::cloud::CloudCommands;
use dozer_orchestrator::cli::generate_config_repl;
use dozer_orchestrator::cli::types::{ApiCommands, AppCommands, Cli, Commands, ConnectorCommands};
use dozer_orchestrator::cli::{init_dozer, init_dozer_with_default_config, list_sources, LOGO};
use dozer_orchestrator::errors::{CliError, OrchestrationError};
use dozer_orchestrator::simple::SimpleOrchestrator;
#[cfg(feature = "cloud")]
use dozer_orchestrator::CloudOrchestrator;
use dozer_orchestrator::{set_ctrl_handler, set_panic_hook, shutdown, Orchestrator};
use dozer_types::models::telemetry::TelemetryConfig;
use dozer_types::tracing::{error, info};
use serde::Deserialize;
use tokio::time;

use std::cmp::Ordering;
use std::process;
use std::time::Duration;

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
    #[serde(rename(deserialize = "latestVersion"))]
    pub latest_version: String,
    #[serde(rename(deserialize = "availableAssets"))]
    pub _available_assets: Vec<String>,
    pub link: String,
}

fn version_to_vector(version: &str) -> Vec<i32> {
    version.split('.').map(|s| s.parse().unwrap()).collect()
}

fn compare_versions(v1: Vec<i32>, v2: Vec<i32>) -> bool {
    for i in 0..v1.len() {
        match v1.get(i).cmp(&v2.get(i)) {
            Ordering::Greater => return true,
            Ordering::Less => return false,
            Ordering::Equal => continue,
        }
    }
    false
}

async fn check_update() {
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    let query = vec![
        ("version", VERSION),
        ("build", std::env::consts::ARCH),
        ("os", std::env::consts::OS),
    ];

    let request_url = "https://metadata.dev.getdozer.io/";

    let client = reqwest::Client::new();

    let mut printed = false;

    loop {
        let response = client
            .get(&request_url.to_string())
            .query(&query)
            .send()
            .await;

        match response {
            Ok(r) => {
                if !printed {
                    let package: DozerPackage = r.json().await.unwrap();
                    let current = version_to_vector(VERSION);
                    let remote = version_to_vector(&package.latest_version);

                    if compare_versions(remote, current) {
                        info!("A new version of Dozer is available.");
                        info!(
                            "You can download v{}, from {}.",
                            package.latest_version, package.link
                        );
                        printed = true;
                    }
                }
            }
            Err(e) => {
                info!("Failed to check for updates: {}", e);
            }
        }
        time::sleep(Duration::from_secs(2 * 60 * 60)).await;
    }
}

fn run() -> Result<(), OrchestrationError> {
    // Reloading trace layer seems impossible, so we are running Cli::parse in a closure
    // and then initializing it after reading the configuration. This is a hacky workaround, but it works.

    let cli = parse_and_generate()?;
    #[cfg(feature = "cloud")]
    let is_cloud_orchestrator = matches!(cli.cmd, Some(Commands::Cloud(_)));
    #[cfg(not(feature = "cloud"))]
    let is_cloud_orchestrator = false;

    let mut dozer = init_orchestrator(&cli, is_cloud_orchestrator)?;

    let (shutdown_sender, shutdown_receiver) = shutdown::new(&dozer.runtime);
    set_ctrl_handler(shutdown_sender);

    // Now we have access to telemetry configuration. Telemetry must be initialized in tokio runtime.
    let _telemetry = dozer.runtime.block_on(async {
        Telemetry::new(Some(&dozer.config.app_name), dozer.config.telemetry.clone())
    });

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
                CloudCommands::Deploy => dozer.deploy(cloud),
                CloudCommands::List(ref list) => dozer.list(cloud, list),
                CloudCommands::Status(ref app) => dozer.status(cloud, app.app_id.clone()),
                CloudCommands::Monitor(ref app) => dozer.monitor(cloud, app.app_id.clone()),
                CloudCommands::Update(ref app) => dozer.update(cloud, app.app_id.clone()),
                CloudCommands::Delete(ref app) => dozer.delete(cloud, app.app_id.clone()),
                CloudCommands::Logs(ref app) => dozer.trace_logs(cloud, app.app_id.clone()),
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

fn init_orchestrator(
    cli: &Cli,
    is_cloud_orchestrator: bool,
) -> Result<SimpleOrchestrator, CliError> {
    dozer_tracing::init_telemetry_closure(None, None, || -> Result<SimpleOrchestrator, CliError> {
        let res = if is_cloud_orchestrator {
            init_dozer_with_default_config()
        } else {
            init_dozer(cli.config_path.clone())
        };

        match res {
            Ok(dozer) => {
                dozer.runtime.spawn(check_update());
                Ok(dozer)
            }
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
