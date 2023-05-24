use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use futures::Future;
use tokio::{
    runtime::Runtime,
    sync::watch::{channel, Receiver, Sender},
};

#[derive(Debug)]
pub struct ShutdownSender(Receiver<()>);

impl ShutdownSender {
    pub fn shutdown(self) {}
}

#[derive(Debug, Clone)]
pub struct ShutdownReceiver {
    sender: Arc<Sender<()>>,
    running: Arc<AtomicBool>,
}

impl ShutdownReceiver {
    pub fn get_running_flag(&self) -> Arc<AtomicBool> {
        self.running.clone()
    }

    pub fn create_shutdown_future(&self) -> impl Future<Output = ()> {
        wait_shutdown(self.sender.clone())
    }
}

async fn wait_shutdown(sender: Arc<Sender<()>>) {
    sender.closed().await;
}

pub fn new(runtime: &Runtime) -> (ShutdownSender, ShutdownReceiver) {
    let (sender, receiver) = channel(());
    let sender = Arc::new(sender);
    let running = Arc::new(AtomicBool::new(true));
    runtime.spawn(wait_and_set_running_flag(sender.clone(), running.clone()));
    (
        ShutdownSender(receiver),
        ShutdownReceiver { sender, running },
    )
}

async fn wait_and_set_running_flag(sender: Arc<Sender<()>>, flag: Arc<AtomicBool>) {
    sender.closed().await;
    flag.store(false, Ordering::SeqCst);
}
