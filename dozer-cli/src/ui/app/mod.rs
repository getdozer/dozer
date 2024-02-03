mod errors;
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
use std::sync::Arc;
use tokio::runtime::Runtime;

const APP_UI_WEB_PORT: u16 = 62888;




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
                Err(_) => Ok(()),
            };

        res.unwrap();
    });
    watcher::watch(runtime, state.clone(), shutdown).await?;

    Ok(())
}
