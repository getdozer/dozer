use crate::dag::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::errors::ExecutionError;
use crate::execution::schema::ExecutionSchema;
use crate::storage::common::{Environment, RwTransaction};
use crate::storage::record_reader::RecordReader;
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;

pub type NodeHandle = String;
pub type PortHandle = u16;

#[derive(Debug, Clone)]
pub struct OutputPortDefOptions {
    pub stateful: bool,
    pub retrieve_old_record_for_updates: bool,
    pub retrieve_old_record_for_deletes: bool,
}

impl OutputPortDefOptions {
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

pub struct OutputPortDef {
    pub handle: PortHandle,
    pub options: OutputPortDefOptions,
}

impl OutputPortDef {
    pub fn new(handle: PortHandle, options: OutputPortDefOptions) -> Self {
        Self { handle, options }
    }
}

pub trait ProcessorFactory: Send + Sync {
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn get_output_ports(&self) -> Vec<OutputPortDef>;
    fn build(&self) -> Box<dyn Processor>;
    fn get_schema(
        &mut self,
        output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, ExecutionSchema>,
    ) -> Result<ExecutionSchema, ExecutionError>;
}

pub trait Processor {
    fn init(&mut self, state: &mut dyn Environment) -> Result<(), ExecutionError>;
    fn commit(&self, tx: &mut dyn RwTransaction) -> Result<(), ExecutionError>;
    fn terminate(&self, tx: &mut dyn RwTransaction) -> Result<(), ExecutionError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        tx: &mut dyn RwTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError>;
}

pub trait SinkFactory: Send + Sync {
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn Sink>;
    fn update_schema(
        &mut self,
        input_schemas: &HashMap<PortHandle, ExecutionSchema>,
    ) -> Result<(), ExecutionError>;
}

pub trait Sink {
    fn init(&mut self, state: &mut dyn Environment) -> Result<(), ExecutionError>;
    fn commit(&self, tx: &mut dyn RwTransaction) -> Result<(), ExecutionError>;
    fn terminate(&self, tx: &mut dyn RwTransaction) -> Result<(), ExecutionError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        seq: u64,
        op: Operation,
        state: &mut dyn RwTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError>;
}
