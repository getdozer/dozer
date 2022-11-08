use crate::dag::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::errors::ExecutionError;
use crate::storage::common::{Environment, RwTransaction};
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;

pub type NodeHandle = String;
pub type PortHandle = u16;

pub trait ProcessorFactory: Send + Sync {
    fn is_stateful(&self) -> bool;
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn get_output_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn Processor>;
}

pub trait SourceFactory: Send + Sync {
    fn get_output_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn Source>;
}

pub trait Source {
    fn get_output_schema(&self, port: PortHandle) -> Option<Schema>;
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        from_seq: Option<u64>,
    ) -> Result<(), ExecutionError>;
}

pub trait Processor {
    fn update_schema(
        &mut self,
        output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError>;
    fn init(&mut self, state: Option<&mut dyn Environment>) -> Result<(), ExecutionError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        state: Option<&mut dyn RwTransaction>,
    ) -> Result<(), ExecutionError>;
}

pub trait SinkFactory: Send + Sync {
    fn is_stateful(&self) -> bool;
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn Sink>;
}

pub trait Sink {
    fn update_schema(
        &mut self,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError>;
    fn init(&mut self, state: Option<&mut dyn Environment>) -> Result<(), ExecutionError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        seq: u64,
        op: Operation,
        state: Option<&mut dyn RwTransaction>,
    ) -> Result<(), ExecutionError>;
}
