mod errors;
mod server;
mod state;
mod watcher;
mod downloader;
use std::sync::Arc;
mod helper;
use dozer_types::{grpc_types::live::LiveResponse, log::info};
pub use errors::LiveError;
use futures::stream::{AbortHandle, Abortable};
use crate::shutdown::ShutdownReceiver;
use std::thread;
use self::state::LiveState;

// const WEB_PORT: u16 = 4555;
pub fn start_live_server(
    runtime: Arc<tokio::runtime::Runtime>,
    shutdown: ShutdownReceiver,
) -> Result<(), LiveError> {
    let (sender, receiver) = tokio::sync::broadcast::channel::<LiveResponse>(100);
    let state = Arc::new(LiveState::new());

    state.build()?;

    let url = "https://dozer-explorer.s3.ap-southeast-1.amazonaws.com/latest";

    let (key, existing_key, key_changed) = downloader::get_key_from_url(url)?;
    let zip_file_name = key.as_str();
    let prev_zip_file_name = existing_key.as_str();
    if key_changed{
        println!("Downloading latest file: {}",zip_file_name);

        let base_url = "https://dozer-explorer.s3.ap-southeast-1.amazonaws.com/";
        let zip_url = &(base_url.to_owned() + zip_file_name);
        if !prev_zip_file_name.is_empty(){
        downloader::delete_file_if_present(prev_zip_file_name)?;
        }
        downloader::get_zip_from_url(zip_url,zip_file_name)?;
    }

    let handle = thread::spawn(|| {
        downloader::start_react_app().unwrap();
    });

    let state2 = state.clone();

    // let browser_url = format!("http://localhost:{}", WEB_PORT);

    // if webbrowser::open(&browser_url).is_err() {
    //     info!("Failed to open browser. Connecto");
    // }

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

    handle.join().unwrap();

    watcher::watch(sender, state.clone(), shutdown)?;
    Ok(())
}
