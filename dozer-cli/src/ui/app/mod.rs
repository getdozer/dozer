mod errors;
mod progress;
mod server;
mod state;
mod watcher;
use crate::ui::{
    app::{server::APP_UI_PORT, state::AppUIState},
    downloader::{self, LOCAL_APP_UI_DIR},
};
use dozer_api::shutdown::ShutdownReceiver;
use dozer_types::{grpc_types::app_ui::ConnectResponse, log::info};
pub use errors::AppUIError;
use futures::stream::{AbortHandle, Abortable};
use std::{process::Stdio, sync::Arc};
use tokio::process::Command;
use tokio::runtime::Runtime;

const APP_UI_WEB_PORT: u16 = 62888;
const GPT_SERVER_PORT: u16 = 62777;
const DOCKER_DOZER_GPT_IMAGE_NAME: &str = "public.ecr.aws/getdozer/dozer-gpt:latest";
const DOCKER_CONTAINER_NAME: &str = "dozer-gpt";

pub async fn start_app_ui_server(
    runtime: &Arc<Runtime>,
    shutdown: ShutdownReceiver,
    disable_ui: bool,
) -> Result<(), AppUIError> {
    let (sender, receiver) = tokio::sync::broadcast::channel::<ConnectResponse>(100);
    let state = Arc::new(AppUIState::new());
    state.set_sender(sender.clone()).await;
    // Ignore if build fails
    let _ = state.build(runtime.clone()).await;
    let state2: Arc<AppUIState> = state.clone();
    if !disable_ui {
        downloader::fetch_latest_dozer_app_ui_code().await?;
        let react_app_server: dozer_api::actix_web::dev::Server =
            downloader::start_react_app(APP_UI_WEB_PORT, LOCAL_APP_UI_DIR)
                .map_err(AppUIError::CannotStartUiServer)?;
        tokio::spawn(react_app_server);
        let browser_url: String = format!("http://localhost:{}", APP_UI_WEB_PORT);
        if webbrowser::open(&browser_url).is_err() {
            info!("Failed to open browser. ");
        }
    }
    info!("Starting app ui server on port : {}", APP_UI_PORT);
    let rshudown = shutdown.clone();
    tokio::spawn(async {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        tokio::spawn(async move {
            rshudown.create_shutdown_future().await;
            abort_handle.abort();
        });
        let res: Result<(), AppUIError> =
            match Abortable::new(server::serve(receiver, state2), abort_registration).await {
                Ok(result) => result.map_err(AppUIError::Transport),
                Err(_) => stop_and_remove_docker_container(DOCKER_CONTAINER_NAME).await,
            };

        res.unwrap();
    });
    watcher::watch(runtime, state.clone(), shutdown).await?;

    Ok(())
}

async fn start_gpt_server() -> Result<(), AppUIError> {
    if is_docker_installed().await {
        return pull_and_start_docker_image(DOCKER_DOZER_GPT_IMAGE_NAME, DOCKER_CONTAINER_NAME)
            .await;
    }
    Err(AppUIError::DockerNotInstalled)
}

async fn is_docker_installed() -> bool {
    let output = Command::new("docker")
        .arg("--version")
        .stdout(Stdio::piped())
        .output();
    match output.await {
        Ok(output) => output.status.success(),
        Err(_) => false,
    }
}

async fn pull_and_start_docker_image(
    docker_image: &str,
    container_name: &str,
) -> Result<(), AppUIError> {
    stop_and_remove_docker_container(container_name).await?;
    // Pull the Docker image
    let mut child_pull = Command::new("docker")
        .arg("pull")
        .arg(docker_image) // Replace with your Docker image name
        .spawn()?;

    // terminate process on port
    let pull_status = child_pull.wait().await?;

    if !pull_status.success() {
        return Err(AppUIError::CannotPullDockerImage(docker_image.to_string()));
    }
    info!("Starting docker container with name : {}", container_name);
    // Run the Docker container
    let mut child_run = Command::new("docker")
        .arg("run")
        .arg("-d") // Run in detached mode
        .arg("--name")
        .arg(container_name) // Name for the container
        .arg("-p")
        .arg(format!("{}:{}", GPT_SERVER_PORT, GPT_SERVER_PORT)) // Port mapping
        .arg(docker_image) // Replace with your Docker image name
        .spawn()?;

    let run_status: std::process::ExitStatus = child_run.wait().await?;
    if !run_status.success() {
        return Err(AppUIError::CannotRunDockerImage(docker_image.to_string()));
    }
    Ok(())
}

async fn stop_and_remove_docker_container(container_name: &str) -> Result<(), AppUIError> {
    info!("Stopping docker container with name : {}", container_name);
    // Stop the Docker container
    let mut child_stop = Command::new("docker")
        .arg("stop")
        .arg(container_name)
        .spawn()?;

    let _stop_status = child_stop.wait().await?;


    // Remove the Docker container
    let mut child_rm = Command::new("docker")
        .arg("rm")
        .arg(container_name)
        .spawn()?;

    let _rm_status = child_rm.wait().await?;

    Ok(())
}
