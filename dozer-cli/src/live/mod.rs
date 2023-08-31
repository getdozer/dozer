mod downloader;
mod errors;
mod server;
mod state;
mod watcher;
use self::state::LiveState;
use crate::{cli::types::Live, live::server::LIVE_PORT, shutdown::ShutdownReceiver};
use dozer_types::{grpc_types::live::ConnectResponse, log::info};
use std::sync::Arc;
mod progress;
pub use errors::LiveError;
use futures::stream::{AbortHandle, Abortable};
use tokio::runtime::Runtime;

const WEB_PORT: u16 = 62999;
pub async fn start_live_server(
    runtime: &Arc<Runtime>,
    shutdown: ShutdownReceiver,
    live_flags: Live,
) -> Result<(), LiveError> {
    let (sender, receiver) = tokio::sync::broadcast::channel::<ConnectResponse>(100);
    let state = Arc::new(LiveState::new());

    state.set_sender(sender.clone()).await;
    // Ignore if build fails
    let _ = state.build(runtime.clone()).await;

    downloader::fetch_latest_dozer_explorer_code().await?;

    if !live_flags.disable_live_ui {
        let react_app_server =
            downloader::start_react_app().map_err(LiveError::CannotStartUiServer)?;
        tokio::spawn(react_app_server);
        let browser_url = format!("http://localhost:{}", WEB_PORT);

        if webbrowser::open(&browser_url).is_err() {
            info!("Failed to open browser. ");
        }
    }

    let state2 = state.clone();

    info!("Starting live server on port : {}", LIVE_PORT);

    let rshudown = shutdown.clone();
    tokio::spawn(async {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        tokio::spawn(async move {
            rshudown.create_shutdown_future().await;
            abort_handle.abort();
        });
        let res = match Abortable::new(server::serve(receiver, state2), abort_registration).await {
            Ok(result) => result.map_err(LiveError::Transport),
            Err(_) => Ok(()),
        };
        res.unwrap();
    });

    watcher::watch(runtime, state.clone(), shutdown).await?;

    Ok(())
}
