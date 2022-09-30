use crate::dag::dag::PortHandle;
use crate::dag::mt_executor::DEFAULT_PORT_ID;
use dozer_types::types::{Operation, OperationEvent};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use crossbeam::channel::Sender;
use crate::state::{StateStore, StateStoresManager};

pub trait ExecutionContext: Send + Sync {}


pub enum NextStep {
    Continue,
    Stop,
}

pub trait ProcessorFactory: Send + Sync {
    fn get_input_ports(&self) -> Option<Vec<PortHandle>>;
    fn get_output_ports(&self) -> Option<Vec<PortHandle>>;
    fn build(&self) -> Box<dyn Processor>;
}

pub trait Processor {
    fn init(&mut self, state: &mut dyn StateStore) -> Result<(), String>;
    fn process(&mut self, from_port: Option<PortHandle>, op: OperationEvent, fw: &dyn ChannelForwarder, state: &mut dyn StateStore)
        -> Result<NextStep, String>;
}

pub trait SourceFactory: Send + Sync {
    fn get_output_ports(&self) -> Option<Vec<PortHandle>>;
    fn build(&self) -> Box<dyn Source>;
}

pub trait Source {
    fn start(&self, fw: &dyn ChannelForwarder, state: &mut dyn StateStore) -> Result<(), String>;
}

pub trait SinkFactory: Send + Sync {
    fn get_input_ports(&self) -> Option<Vec<PortHandle>>;
    fn build(&self) -> Box<dyn Sink>;
}

pub trait Sink {
    fn init(&self, state: &mut dyn StateStore) -> Result<(), String>;
    fn process(
        &self,
        from_port: Option<PortHandle>,
        op: OperationEvent,
        state: &mut dyn StateStore
    ) -> Result<NextStep, String>;
}


pub trait ChannelForwarder {
    fn send(&self, op: OperationEvent, port: Option<PortHandle>) -> Result<(), String>;
    fn terminate(&self) -> Result<(), String>;
}

pub struct LocalChannelForwarder {
    senders: HashMap<PortHandle, Vec<Sender<OperationEvent>>>
}

impl LocalChannelForwarder {
    pub fn new(senders: HashMap<PortHandle, Vec<Sender<OperationEvent>>>) -> Self {
        let mut sync = HashMap::<PortHandle, Mutex<()>>::new();
        for e in &senders {
            sync.insert(*e.0, Mutex::<()>::new(()));
        }
        Self { senders }
    }
}


impl ChannelForwarder for LocalChannelForwarder {

    fn send(&self, op: OperationEvent, port: Option<PortHandle>) -> Result<(), String> {

        let port_id = if port.is_none() {
            DEFAULT_PORT_ID
        } else {
            port.unwrap()
        };

        let senders = self.senders.get(&port_id);
        if senders.is_none() {
            return Err("Invalid output port".to_string());
        }

        if senders.unwrap().len() == 1 {
            senders.unwrap()[0].send(op).map_err(|e| e.to_string())?;
        }
        else {
            for sender in senders.unwrap() {
                sender.send(op.clone()).map_err(|e| e.to_string())?;
            }
        }

        return Ok(());
    }

    fn terminate(&self) -> Result<(), String> {
        for senders in &self.senders {

            for sender in senders.1 {
                sender.send(OperationEvent::new(0, Operation::Terminate))
                    .map_err(|e| e.to_string())?;
            }

            loop {
                let mut is_empty = true;
                for senders in &self.senders {
                    for sender in senders.1 {
                        is_empty |= sender.is_empty();
                    }
                }

                if !is_empty {
                    sleep(Duration::from_millis(250));
                } else {
                    break;
                }
            }
        }

        return Ok(());
    }
}
