use crate::dag::dag::PortHandle;
use dozer_types::types::{Operation, OperationEvent, Schema};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use anyhow::anyhow;
use crossbeam::channel::Sender;
use crate::dag::forwarder::{ChannelManager, ProcessorChannelForwarder, SourceChannelForwarder};
use crate::state::{StateStore, StateStoresManager};

pub trait ExecutionContext: Send + Sync {}


pub enum NextStep {
    Continue,
    Stop,
}

pub trait ProcessorFactory: Send + Sync {
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn get_output_ports(&self) -> Vec<PortHandle>;
    fn get_output_schema(&self, output_port: PortHandle, input_schemas: HashMap<PortHandle, Schema>) -> anyhow::Result<Schema>;
    fn build(&self) -> Box<dyn Processor>;
}

pub trait Processor {
    fn init(&mut self, state: &mut dyn StateStore, input_schemas: HashMap<PortHandle, Schema>) -> anyhow::Result<()>;
    fn process(&mut self, from_port: PortHandle, op: Operation, fw: &dyn ProcessorChannelForwarder, state: &mut dyn StateStore)
        -> anyhow::Result<NextStep>;
}

pub trait SourceFactory: Send + Sync {
    fn get_output_ports(&self) -> Vec<PortHandle>;
    fn get_output_schema(&self, port: PortHandle) -> anyhow::Result<Schema>;
    fn build(&self) -> Box<dyn Source>;
}

pub trait Source {
    fn start(&self, fw: &dyn SourceChannelForwarder, cm: &dyn ChannelManager,
             state: &mut dyn StateStore, from_seq: Option<u64>
    ) -> anyhow::Result<()>;
}

pub trait SinkFactory: Send + Sync {
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn Sink>;
}

pub trait Sink {
    fn init(&mut self, state: &mut dyn StateStore, input_schemas: HashMap<PortHandle, Schema>) -> anyhow::Result<()>;
    fn process(
        &mut self,
        from_port: PortHandle,
        op: OperationEvent,
        state: &mut dyn StateStore
    ) -> anyhow::Result<NextStep>;
}
