use clap::Parser;
use dozer_api::shutdown;
#[cfg(feature = "cloud")]
use dozer_cli::cli::cloud::CloudCommands;
use dozer_cli::cli::types::{Cli, Commands, ConnectorCommand, RunCommands, SecurityCommands};
use dozer_cli::cli::{generate_config_repl, init_config};
use dozer_cli::cli::{init_dozer, list_sources, LOGO};
#[cfg(feature = "cloud")]
use dozer_cli::cloud::{cloud_app_context::CloudAppContext, CloudClient, DozerGrpcCloudClient};
use dozer_cli::errors::{CliError, CloudError, OrchestrationError};
use dozer_cli::{live, set_ctrl_handler, set_panic_hook};
use dozer_tracing::LabelsAndProgress;
use dozer_types::models::config::Config;
use dozer_types::models::telemetry::{TelemetryConfig, TelemetryMetricsConfig};
use dozer_types::serde::Deserialize;
use dozer_types::tracing::{error, error_span, info};
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::time;

use dozer_types::log::{debug, warn};
use std::time::Duration;
use std::{env, process};

fn main() {
    if let Err(e) = run() {
        display_error(&e);
        process::exit(1);
    }
}

fn render_logo() {
    const VERSION: &str = env!("CARGO_PKG_VERSION");

    println!("{LOGO}");
    println!("\nDozer Version: {VERSION}\n");
}

#[derive(Deserialize, Debug)]
#[serde(crate = "dozer_types::serde")]
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
                // We dont show error if error is connection error, because mostly it happens
                // when main thread is shutting down before request completes.
                if !e.is_connect() {
                    warn!("Unable to fetch the latest metadata");
                }

                debug!("Updates check error: {}", e);
            }
        }
        time::sleep(Duration::from_secs(2 * 60 * 60)).await;
    }
}

fn run() -> Result<(), OrchestrationError> {
    // Reloading trace layer seems impossible, so we are running Cli::parse in a closure
    // and then initializing it after reading the configuration. This is a hacky workaround, but it works.

    let cli = parse_and_generate()?;

    let runtime = Arc::new(Runtime::new().map_err(CliError::FailedToCreateTokioRuntime)?);

    let (shutdown_sender, shutdown_receiver) = shutdown::new(&runtime);
    set_ctrl_handler(shutdown_sender);

    set_panic_hook();

    // Run Cloud
    #[cfg(feature = "cloud")]
    if let Commands::Cloud(cloud) = &cli.cmd {
        render_logo();
        let cloud = cloud.clone();
        let res = if let CloudCommands::Login {
            organisation_slug,
            profile_name,
            client_id,
            client_secret,
        } = cloud.command.clone()
        {
            CloudClient::login(
                runtime.clone(),
                cloud,
                organisation_slug,
                profile_name,
                client_id,
                client_secret,
            )
        } else {
            let config = init_configuration(&cli, runtime.clone())?;
            let mut cloud_client = CloudClient::new(config.clone(), runtime.clone());
            match cloud.command.clone() {
                CloudCommands::Deploy(deploy) => {
                    cloud_client.deploy(cloud, deploy, cli.config_paths.clone())
                }
                CloudCommands::Login {
                    organisation_slug: _,
                    profile_name: _,
                    client_id: _,
                    client_secret: _,
                } => unreachable!("This is handled earlier"),
                CloudCommands::Secrets(command) => {
                    cloud_client.execute_secrets_command(cloud, command)
                }
                CloudCommands::Delete => cloud_client.delete(cloud),
                CloudCommands::Status => cloud_client.status(cloud),
                CloudCommands::Monitor => cloud_client.monitor(cloud),
                CloudCommands::Logs(logs) => cloud_client.trace_logs(cloud, logs),
                CloudCommands::Version(version) => cloud_client.version(cloud, version),
                CloudCommands::List(list) => cloud_client.list(cloud, list),
                CloudCommands::SetApp { app_id } => {
                    CloudAppContext::save_app_id(app_id.clone())?;
                    info!("Using \"{app_id}\" app");
                    Ok(())
                }
                CloudCommands::ApiRequestSamples { endpoint } => {
                    cloud_client.print_api_request_samples(cloud, endpoint)
                }
            }
        };
        return dozer_tracing::init_telemetry_closure(
            None,
            &Default::default(),
            || -> Result<(), OrchestrationError> {
                match res {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("{}", e);
                        Err(e)
                    }
                }
            },
        );
    }

    let config = init_configuration(&cli, runtime.clone())?;
    // Now we have access to telemetry configuration. Telemetry must be initialized in tokio runtime.
    let app_id = config.cloud.app_id.as_deref().unwrap_or(&config.app_name);

    // We always enable telemetry when running live.
    let telemetry_config = if matches!(cli.cmd, Commands::Live(_)) {
        TelemetryConfig {
            trace: None,
            metrics: Some(TelemetryMetricsConfig::Prometheus),
        }
    } else {
        config.telemetry.clone()
    };

    let _telemetry = runtime.block_on(async { Telemetry::new(Some(app_id), &telemetry_config) });

    let mut dozer = init_dozer(
        runtime.clone(),
        config.clone(),
        LabelsAndProgress::new(Default::default(), cli.enable_progress),
    )
    .map_err(OrchestrationError::CliError)?;

    // run individual servers
    (match cli.cmd {
        Commands::Run(run) => match run.command {
            Some(RunCommands::Api) => {
                render_logo();

                dozer.run_api(shutdown_receiver)
            }
            Some(RunCommands::App) => {
                render_logo();

                dozer.run_apps(shutdown_receiver, None)
            }
            None => {
                render_logo();
                dozer.run_all(shutdown_receiver, run.locked)
            }
        },
        Commands::Security(security) => match security.command {
            SecurityCommands::GenerateToken => {
                let token = dozer.generate_token(None)?;
                info!("token: {:?} ", token);
                Ok(())
            }
        },
        Commands::Build(build) => {
            let force = build.force.is_some();

            dozer.build(force, shutdown_receiver, build.locked)
        }
        Commands::Connectors(ConnectorCommand { filter }) => dozer.runtime.block_on(list_sources(
            dozer.runtime.clone(),
            cli.config_paths,
            cli.config_token,
            cli.config_overrides,
            cli.ignore_pipe,
            filter,
        )),
        Commands::Clean => dozer.clean(),
        #[cfg(feature = "cloud")]
        Commands::Cloud(_) => {
            panic!("This should not happen as it is handled earlier");
        }
        Commands::Init => {
            panic!("This should not happen as it is handled in parse_and_generate");
        }
        Commands::Live(live_flags) => {
            render_logo();
            dozer.runtime.block_on(live::start_live_server(
                &dozer.runtime,
                shutdown_receiver,
                live_flags,
            ))?;
            Ok(())
        }
    })
    .map_err(|e| {
        let _span = error_span!("OrchestrationError", error = %e);

        e
    })
}

// Some commands dont need to initialize the orchestrator
// This function is used to run those commands
fn parse_and_generate() -> Result<Cli, OrchestrationError> {
    dozer_tracing::init_telemetry_closure(
        None,
        &Default::default(),
        || -> Result<Cli, OrchestrationError> {
            let cli = Cli::parse();

            if let Commands::Init = cli.cmd {
                Telemetry::new(None, &Default::default());
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
        },
    )
}

fn init_configuration(cli: &Cli, runtime: Arc<Runtime>) -> Result<Config, CliError> {
    dozer_tracing::init_telemetry_closure(
        None,
        &Default::default(),
        || -> Result<Config, CliError> {
            let res = runtime.block_on(init_config(
                cli.config_paths.clone(),
                cli.config_token.clone(),
                cli.config_overrides.clone(),
                cli.ignore_pipe,
            ));

            match res {
                Ok(config) => {
                    runtime.spawn(check_update());
                    Ok(config)
                }
                Err(e) => {
                    error!("{}", e);
                    Err(e)
                }
            }
        },
    )
}

fn display_error(e: &OrchestrationError) {
    if let OrchestrationError::CloudError(CloudError::ApplicationNotFound) = &e {
        let description = "Dozer cloud service was not able to find application. \n\n\
        Please check your application id in `dozer-config.cloud.yaml` file.\n\
        To change it, you can manually update file or use \"dozer cloud set-app {app_id}\".";

        error!("{}", description);
    } else {
        error!("{}", e);
    }
}

struct Telemetry();

impl Telemetry {
    fn new(app_name: Option<&str>, config: &TelemetryConfig) -> Self {
        dozer_tracing::init_telemetry(app_name, config);
        Self()
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        dozer_tracing::shutdown_telemetry();
    }
}
