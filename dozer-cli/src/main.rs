use clap::Parser;
use dozer_cli::cli::init_config;
use dozer_cli::cli::init_dozer;
use dozer_cli::cli::types::{Cli, Commands, UICommands};
use dozer_cli::errors::{CliError, CloudError, OrchestrationError};
use dozer_cli::ui;
use dozer_cli::ui::app::AppUIError;
use dozer_cli::{set_ctrl_handler, set_panic_hook};
use dozer_core::shutdown;
use dozer_tracing::DozerMonitorContext;
use dozer_types::models::config::Config;
use dozer_types::tracing::{error, error_span, info};
use futures::TryFutureExt;
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

    let telemetry_config = config_res
        .as_ref()
        .map(|(c, _)| c.telemetry.clone())
        .unwrap_or_default();

    runtime.block_on(async {
        let t = dozer_tracing::Telemetry::new(app_id, &telemetry_config);
        let _r = tokio::spawn(async move { t.serve().await });
    });

    // running UI does not require config to be loaded
    if let Commands::UI(run) = &cli.cmd {
        if let Some(UICommands::Update) = run.command {
            runtime.block_on(
                ui::downloader::fetch_latest_dozer_app_ui_code()
                    .map_err(AppUIError::DownloaderError),
            )?;
            info!("Run `dozer ui` to see the changes.");
        } else {
            runtime.block_on(ui::app::start_app_ui_server(
                &runtime,
                shutdown_receiver,
                false,
            ))?;
        }
        return Ok(());
    }

    let (config, config_files) = config_res?;
    info!("Loaded config from: {}", config_files.join(", "));

    let dozer = init_dozer(
        runtime.clone(),
        config.clone(),
        DozerMonitorContext::new(Default::default(), cli.enable_progress),
    )
    .map_err(OrchestrationError::CliError)?;

    // run individual servers
    (match cli.cmd {
        Commands::Run => dozer
            .runtime
            .block_on(dozer.run_apps(shutdown_receiver, None)),
        Commands::Build(build) => {
            let force = build.force.is_some();

            dozer
                .runtime
                .block_on(dozer.build(force, shutdown_receiver, build.locked))
        }
        Commands::Clean => dozer.clean(),
        Commands::UI(_) => {
            panic!("This should not happen as it is handled earlier");
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

            Ok(cli)
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
