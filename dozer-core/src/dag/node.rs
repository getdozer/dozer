use crate::dag::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::errors::ExecutionError;
use crate::dag::record_store::RecordReader;
use crate::storage::common::{Environment, RwTransaction};
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;

pub type NodeHandle = String;
pub type PortHandle = u16;

pub struct StatefulPortHandle {
    pub handle: PortHandle,
    pub stateful: bool,
}

pub trait StatelessSourceFactory: Send + Sync {
    fn get_output_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn StatelessSource>;
}

pub trait StatelessSource {
    fn get_output_schema(&self, port: PortHandle) -> Option<Schema>;
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        from_seq: Option<u64>,
    ) -> Result<(), ExecutionError>;
}

pub trait StatefulSourceFactory: Send + Sync {
    fn get_output_ports(&self) -> Vec<StatefulPortHandle>;
    fn build(&self) -> Box<dyn StatefulSource>;
}

pub trait StatefulSource {
    fn get_output_schema(&self, port: PortHandle) -> Option<Schema>;
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        from_seq: Option<u64>,
    ) -> Result<(), ExecutionError>;
}

pub trait StatelessProcessorFactory: Send + Sync {
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn get_output_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn StatelessProcessor>;
}

pub trait StatelessProcessor {
    fn init(&mut self) -> Result<(), ExecutionError>;
    fn update_schema(
        &mut self,
        output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError>;
}

impl StatefulPortHandle {
    pub fn new(handle: PortHandle, stateful: bool) -> Self {
        Self { handle, stateful }
    }
}

pub trait StatefulProcessorFactory: Send + Sync {
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn get_output_ports(&self) -> Vec<StatefulPortHandle>;
    fn build(&self) -> Box<dyn StatefulProcessor>;
}

pub trait StatefulProcessor {
    fn init(&mut self, state: &mut dyn Environment) -> Result<(), ExecutionError>;
    fn update_schema(
        &mut self,
        output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        tx: &mut dyn RwTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError>;
}

pub trait StatelessSinkFactory: Send + Sync {
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn StatelessSink>;
}

pub trait StatelessSink {
    fn update_schema(
        &mut self,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError>;
    fn init(&mut self) -> Result<(), ExecutionError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        seq: u64,
        op: Operation,
    ) -> Result<(), ExecutionError>;
}

pub trait StatefulSinkFactory: Send + Sync {
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn StatefulSink>;
}

pub trait StatefulSink {
    fn update_schema(
        &mut self,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError>;
    fn init(&mut self, state: &mut dyn Environment) -> Result<(), ExecutionError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        seq: u64,
        op: Operation,
        state: &mut dyn RwTransaction,
    ) -> Result<(), ExecutionError>;
}
