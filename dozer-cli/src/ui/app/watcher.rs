use std::{sync::Arc, time::Duration};

use super::{
    state::{AppUIState, BroadcastType},
    AppUIError,
};

use dozer_api::shutdown::ShutdownReceiver;
use dozer_types::log::info;
use notify::{RecursiveMode, Watcher};
use notify_debouncer_full::new_debouncer;
use tokio::{runtime::Runtime, select};

pub async fn watch(
    runtime: &Arc<Runtime>,
    state: Arc<AppUIState>,
    shutdown: ShutdownReceiver,
) -> Result<(), AppUIError> {
    // setup debouncer
    let (tx, rx) = std::sync::mpsc::channel();

    let dir: std::path::PathBuf = std::env::current_dir()?;
    let mut debouncer = new_debouncer(Duration::from_millis(500), None, tx)?;
    debouncer
        .cache()
        .add_root(dir.as_path(), RecursiveMode::Recursive);
    let watcher = debouncer.watcher();

    watcher.watch(dir.as_path(), RecursiveMode::NonRecursive)?;

    let additional_paths = vec![dir.join("sql")];

    for path in additional_paths {
        let _ = watcher.watch(path.as_path(), RecursiveMode::NonRecursive);
    }

    let (async_sender, mut async_receiver) = tokio::sync::mpsc::channel(10);

    // Thread that adapts the sync watcher channel to an async channel
    let adapter = runtime.spawn_blocking(move || loop {
        let res = rx.recv();
        let Ok(msg) = res else {
            break;
        };
        let _ = async_sender.blocking_send(msg);
    });

    loop {
        select! {
            Some(msg) = async_receiver.recv() => match msg {
                Ok(_events) => {
                    build(runtime.clone(), state.clone()).await;
                }
                Err(errors) => errors.iter().for_each(|error| info!("{error:?}")),
            },
            // We are shutting down
            _ = shutdown.create_shutdown_future() => break,
            // The watcher quit
            else => break
        }
    }

    // Drop the channels that may keep the adapter thread alive
    drop(async_receiver);
    drop(debouncer);

    let _ = adapter.await;

    Ok(())
}

async fn build(runtime: Arc<Runtime>, state: Arc<AppUIState>) {
    state.broadcast(BroadcastType::Start).await;
    if let Err(res) = state.build(runtime).await {
        let message = res.to_string();
        state.broadcast(BroadcastType::Failed(message)).await;
    } else {
        state.broadcast(BroadcastType::Success).await;
    }
}
