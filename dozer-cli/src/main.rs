use clap::Parser;
#[cfg(feature = "cloud")]
use dozer_cli::cli::cloud::{CloudCommands, OrganisationCommand};
use dozer_cli::cli::generate_config_repl;
use dozer_cli::cli::types::{
    ApiCommands, AppCommands, Cli, Commands, ConnectorCommand, RunCommands, SecurityCommands,
};
use dozer_cli::cli::{init_dozer, list_sources, LOGO};
use dozer_cli::errors::{CliError, OrchestrationError};
use dozer_cli::simple::SimpleOrchestrator;
#[cfg(feature = "cloud")]
use dozer_cli::CloudOrchestrator;
use dozer_cli::{set_ctrl_handler, set_panic_hook, shutdown, Orchestrator};
use dozer_types::models::telemetry::TelemetryConfig;
use dozer_types::tracing::{error, info};
use serde::Deserialize;
use tokio::time;

use clap::CommandFactory;
#[cfg(feature = "cloud")]
use dozer_cli::cloud_app_context::CloudAppContext;
use dozer_types::log::warn;
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
    const VERSION: &str = env!("CARGO_PKG_VERSION");

    println!("{LOGO}");
    println!("\nDozer Version: {VERSION}\n");
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
    let dozer_env = std::env::var("DOZER_ENV").unwrap_or("local".to_string());
    let dozer_dev = std::env::var("DOZER_DEV").unwrap_or("ext".to_string());
    let query = vec![
        ("version", VERSION),
        ("build", std::env::consts::ARCH),
        ("os", std::env::consts::OS),
        ("env", &dozer_env),
        ("dev", &dozer_dev),
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
    let mut dozer = init_orchestrator(&cli)?;

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
                    warn!("DEPRECATED. Please use \"dozer run api\" command");
                    render_logo();

                    dozer.run_api(shutdown_receiver)
                }
                ApiCommands::GenerateToken => {
                    warn!("DEPRECATED. Please use \"dozer security generate-token\" command");

                    let token = dozer.generate_token()?;
                    info!("token: {:?} ", token);
                    Ok(())
                }
            },
            Commands::App(apps) => match apps.command {
                AppCommands::Run => {
                    warn!("DEPRECATED. Please use \"dozer run app\" command");
                    render_logo();

                    dozer.run_apps(shutdown_receiver, None, None)
                }
            },
            Commands::Run(run) => match run.command {
                RunCommands::Api => {
                    render_logo();

                    dozer.run_api(shutdown_receiver)
                }
                RunCommands::App => {
                    render_logo();

                    dozer.run_apps(shutdown_receiver, None, None)
                }
            },
            Commands::Security(security) => match security.command {
                SecurityCommands::GenerateToken => {
                    let token = dozer.generate_token()?;
                    info!("token: {:?} ", token);
                    Ok(())
                }
            },
            Commands::Build(build) => {
                let force = build.force.is_some();

                dozer.build(force)
            }
            Commands::Connectors(ConnectorCommand { filter }) => {
                list_sources(cli.config_paths, cli.config_token, filter)
            }
            Commands::Clean => dozer.clean(),
            #[cfg(feature = "cloud")]
            Commands::Cloud(cloud) => match cloud.command.clone() {
                CloudCommands::Deploy(deploy) => dozer.deploy(cloud, deploy),
                CloudCommands::Api(api) => dozer.api(cloud, api),
                CloudCommands::Login(OrganisationCommand { organisation_name }) => {
                    dozer.login(cloud, organisation_name)
                }
                CloudCommands::Secrets(command) => dozer.execute_secrets_command(cloud, command),
                CloudCommands::Delete => dozer.delete(cloud),
                CloudCommands::Status => dozer.status(cloud),
                CloudCommands::Monitor => dozer.monitor(cloud),
                CloudCommands::Logs(logs) => dozer.trace_logs(cloud, logs),
                CloudCommands::Version(version) => dozer.version(cloud, version),
                CloudCommands::List(list) => dozer.list(cloud, list),
                CloudCommands::SetApp { app_id } => {
                    CloudAppContext::save_app_id(app_id.clone())?;
                    info!("Using \"{app_id}\" app");
                    Ok(())
                }
            },
            Commands::Init => {
                panic!("This should not happen as it is handled in parse_and_generate");
            }
        }
    } else {
        render_logo();

        dozer.run_all(shutdown_receiver, None)
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
        let res = init_dozer(cli.config_paths.clone(), cli.config_token.clone());

        match res {
            Ok(dozer) => {
                dozer.runtime.spawn(check_update());
                Ok(dozer)
            }
            Err(e) => {
                if let CliError::FailedToFindConfigurationFiles(_) = &e {
                    let description = "Dozer was not able to find configuration files. \n\n\
                    Please use \"dozer init\" to create project or \"dozer -c {path}\" with path to your configuration.\n\
                    Configuration documentation can be found in https://getdozer.io/docs/configuration";

                    let mut command = Cli::command();
                    command = command.about(format!("\n\n\n{} \n {}", LOGO, description));

                    println!("{}", command.render_help());
                }

                error!("{}", e);
                Err(e)
            }
        }
    })
}

struct Telemetry();

impl Telemetry {
    fn new(app_name: Option<&str>, config: Option<TelemetryConfig>) -> Self {
        dozer_tracing::init_telemetry(app_name, config);
        Self()
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        dozer_tracing::shutdown_telemetry();
    }
}
