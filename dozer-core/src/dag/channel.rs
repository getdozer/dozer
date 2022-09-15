use crossbeam::channel::{bounded, Receiver, Sender};
use dozer_shared::types::OperationEvent;

/*****************************************************************************
  Channel Traits
******************************************************************************/

pub trait NodeSender: Send + Sync {
    fn send(&self, op: OperationEvent) -> Result<(), String>;
    fn is_empty(&self) -> bool;
}

pub trait NodeReceiver: Send + Sync {
    fn receive(&self) -> Result<OperationEvent, String>;
}

pub trait NodeChannel: Send + Sync {
    fn build(&self) -> (Box<dyn NodeSender>, Box<dyn NodeReceiver>);
}

/*****************************************************************************
  LocalNodeSender
******************************************************************************/

pub struct LocalNodeSender {
    sender: Sender<OperationEvent>
}

impl LocalNodeSender {
    pub fn new(sender: Sender<OperationEvent>) -> Self {
        Self { sender }
    }
}

impl NodeSender for LocalNodeSender {
    fn send(&self, op: OperationEvent) -> Result<(), String> {
        let sent = self.sender.send(op);
        if sent.is_err() {
            Err(sent.err().unwrap().to_string()) }
        else {
            return Ok(())
        }
    }

    fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }
}

/*****************************************************************************
  LocalNodeReceiver
******************************************************************************/

pub struct LocalNodeReceiver {
    receiver: Receiver<OperationEvent>
}

impl NodeReceiver for LocalNodeReceiver {
    fn receive(&self) -> Result<OperationEvent, String> {
        let received = self.receiver.recv();
        if received.is_err() { Err(received.err().unwrap().to_string())} else { Ok(received.unwrap())}
    }
}

impl LocalNodeReceiver {
    pub fn new(receiver: Receiver<OperationEvent>) -> Self {
        Self { receiver }
    }
}

/*****************************************************************************
  LocalNodeChannel
******************************************************************************/

pub struct LocalNodeChannel {
    size: usize
}

impl LocalNodeChannel {
    pub fn new(size: usize) -> Self {
        Self { size }
    }
}


impl NodeChannel for LocalNodeChannel {

    fn build(&self) -> (Box<dyn NodeSender>, Box<dyn NodeReceiver>) {
        let (s, r) = bounded::<OperationEvent>(self.size);
        (
            Box::new(LocalNodeSender::new(s)),
            Box::new(LocalNodeReceiver::new(r))
        )
    }
}

