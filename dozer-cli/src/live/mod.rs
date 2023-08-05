mod errors;
mod server;
mod state;
mod watcher;
use std::sync::Arc;

use dozer_types::grpc_types::live::LiveResponse;
pub use errors::LiveError;

use self::state::LiveState;

pub fn start_live_server() -> Result<(), LiveError> {
    let (sender, receiver) = tokio::sync::broadcast::channel::<LiveResponse>(100);

    let state = Arc::new(LiveState::new());

    state.build()?;

    let state2 = state.clone();
    std::thread::spawn(move || {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(server::serve(receiver, state2));
    });

    watcher::watch(sender, state.clone())?;
    Ok(())
}
