use std::{sync::Arc, time::Duration};

use crate::shutdown::ShutdownReceiver;

use super::{state::LiveState, LiveError};

use crate::live::state::BroadcastType;
use dozer_types::log::info;
use notify::{RecursiveMode, Watcher};
use notify_debouncer_full::new_debouncer;
use tokio::runtime::Runtime;

pub async fn watch(
    runtime: &Arc<Runtime>,
    state: Arc<LiveState>,
    shutdown: ShutdownReceiver,
) -> Result<(), LiveError> {
    // setup debouncer
    let (tx, rx) = std::sync::mpsc::channel();

    let dir: std::path::PathBuf = std::env::current_dir()?;
    let mut debouncer = new_debouncer(Duration::from_millis(500), None, tx)?;

    let watcher = debouncer.watcher();

    watcher.watch(dir.as_path(), RecursiveMode::NonRecursive)?;

    let additional_paths = vec![dir.join("sql")];

    for path in additional_paths {
        let _ = watcher.watch(path.as_path(), RecursiveMode::NonRecursive);
    }

    debouncer
        .cache()
        .add_root(dir.as_path(), RecursiveMode::Recursive);

    let running = shutdown.get_running_flag().clone();
    loop {
        let event = rx.recv_timeout(Duration::from_millis(100));
        match event {
            Ok(result) => match result {
                Ok(_events) => {
                    info!("Rebuilding....");
                    info!("Events: {:?}", _events);
                    build(runtime.clone(), state.clone()).await;
                }
                Err(errors) => errors.iter().for_each(|error| info!("{error:?}")),
            },
            Err(e) => {
                if !running.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                if e == std::sync::mpsc::RecvTimeoutError::Disconnected {
                    break;
                }
            }
        }
    }

    Ok(())
}

async fn build(runtime: Arc<Runtime>, state: Arc<LiveState>) {
    state.broadcast(BroadcastType::BuildStart).await;
    if let Err(res) = state.build(runtime).await {
        let message = res.to_string();
        state.set_error_message(Some(message.clone())).await;
        state.broadcast(BroadcastType::BuildFailed(message)).await;
    } else {
        state.set_error_message(None).await;
        state.broadcast(BroadcastType::BuildSuccess).await;
    }
    info!("Broadcasting state");
}
