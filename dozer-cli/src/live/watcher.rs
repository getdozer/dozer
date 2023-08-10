use std::{sync::Arc, time::Duration};

use crate::shutdown::ShutdownReceiver;

use super::{state::LiveState, LiveError};

use dozer_types::{grpc_types::live::LiveResponse, log::info};
use notify::{RecursiveMode, Watcher};
use notify_debouncer_full::new_debouncer;
use tokio::sync::broadcast::Sender;

pub fn watch(
    sender: Sender<LiveResponse>,
    state: Arc<LiveState>,
    shutdown: ShutdownReceiver,
) -> Result<(), LiveError> {
    // setup debouncer
    let (tx, rx) = std::sync::mpsc::channel();

    let dir: std::path::PathBuf = std::env::current_dir()?;
    // no specific tickrate, max debounce time 2 seconds
    let mut debouncer = new_debouncer(Duration::from_secs(2), None, tx)?;

    debouncer
        .watcher()
        .watch(dir.as_path(), RecursiveMode::Recursive)?;

    debouncer
        .cache()
        .add_root(dir.as_path(), RecursiveMode::Recursive);

    let running = shutdown.get_running_flag().clone();
    loop {
        let event = rx.recv_timeout(Duration::from_millis(100));
        match event {
            Ok(result) => match result {
                Ok(_events) => {
                    build(sender.clone(), state.clone())?;
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

pub fn build(sender: Sender<LiveResponse>, state: Arc<LiveState>) -> Result<(), LiveError> {
    let res = state.build();

    match res {
        Ok(_) => {
            let res = state.get_current()?;
            sender.send(res).unwrap();
            Ok(())
        }
        Err(e) => Err(e),
    }
}
