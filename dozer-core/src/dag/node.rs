use crate::dag::dag::PortHandle;
use crate::dag::mt_executor::DEFAULT_PORT_ID;
use dozer_types::types::{Operation, OperationEvent};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use crossbeam::channel::Sender;

pub trait ExecutionContext: Send + Sync {}


pub enum NextStep {
    Continue,
    Stop,
}

pub trait Processor: Send + Sync {
    fn get_input_ports(&self) -> Option<Vec<PortHandle>>;
    fn get_output_ports(&self) -> Option<Vec<PortHandle>>;
    fn init(&self) -> Result<(), String>;
    fn process(
        &self,
        from_port: Option<PortHandle>,
        op: OperationEvent,
        ctx: &dyn ExecutionContext,
        fw: &dyn ChannelForwarder,
    ) -> Result<NextStep, String>;
}

pub trait Source: Send + Sync {
    fn get_output_ports(&self) -> Option<Vec<PortHandle>>;
    fn init(&self) -> Result<(), String>;
    fn start(&self, fw: &dyn ChannelForwarder) -> Result<(), String>;
}

pub trait Sink: Send + Sync {
    fn get_input_ports(&self) -> Option<Vec<PortHandle>>;
    fn init(&self) -> Result<(), String>;
    fn process(
        &self,
        from_port: Option<PortHandle>,
        op: OperationEvent,
        ctx: &dyn ExecutionContext,
    ) -> Result<NextStep, String>;
}


pub struct ProcessorExecutor {
    processor: Arc<dyn Processor>,
    thread_safe: bool,
    sync: Option<Mutex<()>>,
}

impl ProcessorExecutor {
    pub fn new(processor: Arc<dyn Processor>, thread_safe: bool) -> Self {
        Self {
            processor: processor.clone(),
            thread_safe,
            sync: if thread_safe {
                Some(Mutex::new(()))
            } else {
                None
            },
        }
    }

    pub fn process(
        &self,
        port: Option<u8>,
        op: OperationEvent,
        ctx: &dyn ExecutionContext,
        fw: &dyn ChannelForwarder,
    ) -> Result<NextStep, String> {
        if self.thread_safe {
            self.sync.as_ref().unwrap().lock().unwrap();
            return self.processor.process(port, op, ctx, fw);
        } else {
            return self.processor.process(port, op, ctx, fw);
        }
    }
}


pub struct SinkExecutor {
    processor: Arc<dyn Sink>,
    thread_safe: bool,
    sync: Option<Mutex<()>>,
}

impl SinkExecutor {
    pub fn new(processor: Arc<dyn Sink>, thread_safe: bool) -> Self {
        Self {
            processor: processor.clone(),
            thread_safe,
            sync: if thread_safe {
                Some(Mutex::new(()))
            } else {
                None
            },
        }
    }

    pub fn process(
        &self,
        port: Option<u8>,
        op: OperationEvent,
        ctx: &dyn ExecutionContext,
    ) -> Result<NextStep, String> {
        if self.thread_safe {
            self.sync.as_ref().unwrap().lock().unwrap();
            return self.processor.process(port, op, ctx);
        } else {
            return self.processor.process(port, op, ctx);
        }
    }
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
