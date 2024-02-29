use tokio::sync::broadcast::{Receiver, Sender};

use crate::{epoch::Epoch, node::NodeHandle};

#[derive(Debug, Clone)]
pub enum Event {
    SinkCommitted { node: NodeHandle, epoch: Epoch },
}

#[derive(Debug)]
pub struct EventHub {
    pub sender: Sender<Event>,
    pub receiver: Receiver<Event>,
}

impl EventHub {
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = tokio::sync::broadcast::channel(capacity);
        Self { sender, receiver }
    }
}

impl Clone for EventHub {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            receiver: self.sender.subscribe(),
        }
    }
}
