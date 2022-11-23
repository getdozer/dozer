use crate::dag::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::errors::ExecutionError;
use crate::dag::record_store::RecordReader;
use crate::storage::common::{Environment, RwTransaction};
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;

pub type NodeHandle = String;
pub type PortHandle = u16;

#[derive(Debug, Clone)]
pub struct StatefulPortHandleOptions {
    pub stateful: bool,
    pub retrieve_old_record_for_updates: bool,
    pub retrieve_old_record_for_deletes: bool,
}

impl StatefulPortHandleOptions {
    pub fn default() -> Self {
        Self {
            stateful: false,
            retrieve_old_record_for_updates: false,
            retrieve_old_record_for_deletes: false,
        }
    }
    pub fn new(
        stateful: bool,
        retrieve_old_record_for_updates: bool,
        retrieve_old_record_for_deletes: bool,
    ) -> Self {
        Self {
            stateful,
            retrieve_old_record_for_updates,
            retrieve_old_record_for_deletes,
        }
    }
}

pub struct StatefulPortHandle {
    pub handle: PortHandle,
    pub options: StatefulPortHandleOptions,
}

impl StatefulPortHandle {
    pub fn new(handle: PortHandle, options: StatefulPortHandleOptions) -> Self {
        Self { handle, options }
    }
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
        reader: &HashMap<PortHandle, RecordReader>,
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
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError>;
}
