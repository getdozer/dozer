use std::{sync::Arc, time::Duration};

use super::{state::LiveState, LiveError};

use dozer_types::grpc_types::live::LiveResponse;
use notify::{RecursiveMode, Watcher};
use notify_debouncer_full::new_debouncer;
use tokio::sync::broadcast::Sender;

pub fn watch(sender: Sender<LiveResponse>, state: Arc<LiveState>) -> Result<(), LiveError> {
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

    // print all events and errors
    for result in rx {
        match result {
            Ok(_events) => {
                build(sender.clone(), state.clone())?;
            }
            Err(errors) => errors.iter().for_each(|error| println!("{error:?}")),
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
