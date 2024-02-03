use clap::Parser;
use dozer_api::shutdown;
use dozer_cli::cli::cloud::CloudCommands;
use dozer_cli::cli::types::{Cli, Commands, ConnectorCommand, RunCommands, SecurityCommands};
use dozer_cli::cli::{generate_config_repl, init_config};
use dozer_cli::cli::{init_dozer, list_sources};
use dozer_cli::cloud::{cloud_app_context::CloudAppContext, CloudClient, DozerGrpcCloudClient};
use dozer_cli::errors::{CliError, CloudError, OrchestrationError};
use dozer_cli::ui;
use dozer_cli::{set_ctrl_handler, set_panic_hook, ui::live};
use dozer_tracing::LabelsAndProgress;
use dozer_types::models::config::Config;
use dozer_types::models::telemetry::{TelemetryConfig, TelemetryMetricsConfig};
use dozer_types::tracing::{error, error_span, info};
use futures::stream::{AbortHandle, Abortable};
use std::convert::identity;
use std::process;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn main() {
    if let Err(e) = run() {
        display_error(&e);
        process::exit(1);
    }
}

fn run() -> Result<(), OrchestrationError> {
    // Reloading trace layer seems impossible, so we are running Cli::parse in a closure
    // and then initializing it after reading the configuration. This is a hacky workaround, but it works.

    let cli = parse_and_generate()?;

    let runtime = Arc::new(Runtime::new().map_err(CliError::FailedToCreateTokioRuntime)?);

    let (shutdown_sender, shutdown_receiver) = shutdown::new(&runtime);

    runtime.block_on(set_ctrl_handler(shutdown_sender));

    set_panic_hook();

    let config_res = init_configuration(&cli, runtime.clone());
    // Now we have access to telemetry configuration. Telemetry must be initialized in tokio runtime.
    let app_id = config_res
        .as_ref()
        .map(|(c, _)| c.cloud.app_id.as_deref().unwrap_or(&c.app_name))
        .ok();

    // We always enable telemetry when running live.
    let telemetry_config = if matches!(cli.cmd, Commands::Live(_) | Commands::Run(_)) {
        TelemetryConfig {
            trace: None,
            metrics: Some(TelemetryMetricsConfig::Prometheus),
        }
    } else {
        config_res
            .as_ref()
            .map(|(c, _)| c.telemetry.clone())
            .unwrap_or_default()
    };

    let _telemetry = runtime.block_on(async { Telemetry::new(app_id, &telemetry_config) });

    // Run Cloud
    if let Commands::Cloud(cloud) = &cli.cmd {
        return run_cloud(cloud, runtime, &cli);
    }
    let (config, config_files) = config_res?;
    info!("Loaded config from: {}", config_files.join(", "));

    let dozer = init_dozer(
        runtime.clone(),
        config.clone(),
        LabelsAndProgress::new(Default::default(), cli.enable_progress),
    )
    .map_err(OrchestrationError::CliError)?;

    // run individual servers
    (match cli.cmd {
        Commands::Run(run) => match run.command {
            Some(RunCommands::Api) => dozer.runtime.block_on(dozer.run_api(shutdown_receiver)),
            Some(RunCommands::App) => dozer
                .runtime
                .block_on(dozer.run_apps(shutdown_receiver, None)),
            Some(RunCommands::Lambda) => {
                dozer.runtime.block_on(dozer.run_lambda(shutdown_receiver))
            }
            Some(RunCommands::AppUI) => {
                dozer.runtime.block_on(ui::app::start_app_ui_server(
                    &dozer.runtime,
                    shutdown_receiver,
                    false,
                ))?;
                Ok(())
            }
            None => dozer
                .runtime
                .block_on(dozer.run_all(shutdown_receiver, run.locked)),
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

            dozer
                .runtime
                .block_on(dozer.build(force, shutdown_receiver, build.locked))
        }
        Commands::Connectors(ConnectorCommand { filter }) => dozer.runtime.block_on(async {
            let (abort_handle, registration) = AbortHandle::new_pair();
            tokio::spawn(async move {
                shutdown_receiver.create_shutdown_future().await;
                abort_handle.abort();
            });
            Abortable::new(
                list_sources(
                    dozer.runtime.clone(),
                    cli.config_paths,
                    cli.config_token,
                    cli.config_overrides,
                    cli.ignore_pipe,
                    filter,
                ),
                registration,
            )
            .await
            .map_err(|_| OrchestrationError::Aborted)
            // This is basically Result::flatten
            .and_then(identity)
        }),
        Commands::Clean => dozer.clean(),
        Commands::Cloud(_) => {
            panic!("This should not happen as it is handled earlier");
        }
        Commands::Init => {
            panic!("This should not happen as it is handled in parse_and_generate");
        }
        Commands::Live(live_flags) => {
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

fn run_cloud(
    cloud: &dozer_cli::cli::cloud::Cloud,
    runtime: Arc<Runtime>,
    cli: &Cli,
) -> Result<(), OrchestrationError> {
    let cloud = cloud.clone();

    let config = init_configuration(cli, runtime.clone())
        .ok()
        .map(|(config, _)| config);
    let mut cloud_client = CloudClient::new(cloud.clone(), config.clone(), runtime.clone());
    match cloud.command.clone() {
        CloudCommands::Deploy(deploy) => cloud_client.deploy(deploy, cli.config_paths.clone()),
        CloudCommands::Login {
            organisation_slug,
            profile_name,
            client_id,
            client_secret,
        } => cloud_client.login(organisation_slug, profile_name, client_id, client_secret),
        CloudCommands::Secrets(command) => cloud_client.execute_secrets_command(command),
        CloudCommands::Delete => cloud_client.delete(),
        CloudCommands::Status => cloud_client.status(),
        CloudCommands::Monitor => cloud_client.monitor(),
        CloudCommands::Logs(logs) => cloud_client.trace_logs(logs),
        CloudCommands::Version(version) => cloud_client.version(version),
        CloudCommands::List(list) => cloud_client.list(list),
        CloudCommands::SetApp { app_id } => {
            CloudAppContext::save_app_id(app_id.clone())?;
            info!("Using \"{app_id}\" app");
            Ok(())
        }
        CloudCommands::ApiRequestSamples { endpoint } => {
            cloud_client.print_api_request_samples(endpoint)
        }
    }
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

fn init_configuration(cli: &Cli, runtime: Arc<Runtime>) -> Result<(Config, Vec<String>), CliError> {
    dozer_tracing::init_telemetry_closure(None, &Default::default(), || -> Result<_, CliError> {
        runtime.block_on(init_config(
            cli.config_paths.clone(),
            cli.config_token.clone(),
            cli.config_overrides.clone(),
            cli.ignore_pipe,
        ))
    })
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
