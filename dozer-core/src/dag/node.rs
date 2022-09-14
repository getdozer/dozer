use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use dozer_shared::types::{OperationEvent};
use crate::dag::channel::NodeSender;
use crate::dag::dag::PortHandle;
use crate::dag::executor::DEFAULT_PORT_ID;

pub trait ExecutionContext : Send + Sync {

}

/*****************************************************************************
  Processor traits
******************************************************************************/

pub enum NextStep {
    Continue, Stop
}

pub trait Processor : Send + Sync {
    fn get_input_ports(&self) -> Option<Vec<PortHandle>>;
    fn get_output_ports(&self) -> Option<Vec<PortHandle>>;
    fn init(&self) -> Result<(), String>;
    fn process(&self, from_port: Option<PortHandle>, op: OperationEvent, ctx: & dyn ExecutionContext, fw: &ChannelForwarder) -> Result<NextStep, String>;
}

pub trait Source : Send + Sync  {
    fn get_output_ports(&self) -> Option<Vec<PortHandle>>;
    fn init(&self) -> Result<(), String>;
    fn start(&self, fw: &ChannelForwarder) -> Result<(), String>;
}

pub trait Sink : Send + Sync  {
    fn get_input_ports(&self) -> Option<Vec<PortHandle>>;
    fn init(&self) -> Result<(), String>;
    fn process(&self, from_port: Option<PortHandle>, op: OperationEvent, ctx: & dyn ExecutionContext) -> Result<NextStep, String>;
}


/*****************************************************************************
  ProcessorExecutor
******************************************************************************/

pub struct ProcessorExecutor {
    processor: Arc<dyn Processor>,
    thread_safe: bool,
    sync: Option<Mutex<()>>
}

impl ProcessorExecutor {

    pub fn new(processor: Arc<dyn Processor>, thread_safe: bool) -> Self {
        Self {
            processor: processor.clone(),
            thread_safe,
            sync: if thread_safe {Some(Mutex::new(()))} else {None}
        }
    }

    pub fn process(&self, port: Option<u8>, op: OperationEvent, ctx: & dyn ExecutionContext, fw: &ChannelForwarder) -> Result<NextStep, String> {
        if self.thread_safe {
            self.sync.as_ref().unwrap().lock().unwrap();
            return self.processor.process(port, op, ctx, fw);
        }
        else {
            return self.processor.process(port, op, ctx, fw);
        }
    }
}

/*****************************************************************************
  SinkExecutor
******************************************************************************/

pub struct SinkExecutor {
    processor: Arc<dyn Sink>,
    thread_safe: bool,
    sync: Option<Mutex<()>>
}

impl SinkExecutor {

    pub fn new(processor: Arc<dyn Sink>, thread_safe: bool) -> Self {
        Self {
            processor: processor.clone(),
            thread_safe,
            sync: if thread_safe {Some(Mutex::new(()))} else {None}
        }
    }

    pub fn process(&self, port: Option<u8>, op: OperationEvent, ctx: & dyn ExecutionContext) -> Result<NextStep, String> {
        if self.thread_safe {
            self.sync.as_ref().unwrap().lock().unwrap();
            return self.processor.process(port, op, ctx);
        }
        else {
            return self.processor.process(port, op, ctx);
        }
    }
}

/*****************************************************************************
  ChannelForwarder
******************************************************************************/

pub struct ChannelForwarder {
    senders: HashMap<PortHandle, Vec<Box<dyn NodeSender>>>,
    thread_safe: bool,
    sync: HashMap<PortHandle, Mutex<()>>
}

impl ChannelForwarder {

    pub fn new(senders: HashMap<PortHandle, Vec<Box<dyn NodeSender>>>, thread_safe: bool) -> Self {

        let mut sync = HashMap::<PortHandle, Mutex<()>>::new();
        for e in &senders {
            sync.insert(*e.0, Mutex::<()>::new(()));
        }
        Self { senders, thread_safe, sync}
    }

    pub fn send(&self, op: OperationEvent, port: Option<PortHandle>) -> Result<(),String> {

        let port_id = if port.is_none() { DEFAULT_PORT_ID } else { port.unwrap() };

        let senders = self.senders.get(&port_id);
        if senders.is_none() {
            return Err("Invalid output port".to_string());
        }

        if self.thread_safe {
            self.sync.get(&port_id).unwrap().lock().unwrap();
            for sender in senders.unwrap() {
                sender.send(op.clone())?;
            }
        }
        else {
            for sender in senders.unwrap() {
                sender.send(op.clone())?;
            }
        }
        return Ok(());
    }

}
