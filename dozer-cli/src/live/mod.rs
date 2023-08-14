mod downloader;
mod errors;
mod server;
mod state;
mod watcher;
use std::sync::Arc;
mod helper;
use self::state::LiveState;
use crate::shutdown::ShutdownReceiver;
use dozer_types::{grpc_types::live::LiveResponse, log::info};
pub use errors::LiveError;
use futures::stream::{AbortHandle, Abortable};

const WEB_PORT: u16 = 3000;
pub fn start_live_server(
    runtime: Arc<tokio::runtime::Runtime>,
    shutdown: ShutdownReceiver,
) -> Result<(), LiveError> {
    let (sender, receiver) = tokio::sync::broadcast::channel::<LiveResponse>(100);
    let state = Arc::new(LiveState::new());

    state.build()?;

    runtime.block_on(downloader::fetch_latest_dozer_explorer_code())?;

    let react_app_server = downloader::start_react_app().map_err(LiveError::CannotStartUiServer)?;
    runtime.spawn(react_app_server);

    let state2 = state.clone();

    let browser_url = format!("http://localhost:{}", WEB_PORT);

    if webbrowser::open(&browser_url).is_err() {
        info!("Failed to open browser. Connecto");
    }

    info!("Starting live server");

    let rshudown = shutdown.clone();
    runtime.spawn(async {
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

    watcher::watch(sender, state.clone(), shutdown)?;

    Ok(())
}
