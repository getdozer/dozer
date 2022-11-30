use crate::dag::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::errors::ExecutionError;
use crate::execution::schema::ExecutionSchema;
use crate::storage::common::{Environment, RwTransaction};
use crate::storage::record_reader::RecordReader;
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;

pub type NodeHandle = String;
pub type PortHandle = u16;

pub trait ProcessorFactory: Send + Sync {
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn get_output_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn Processor>;
    fn get_output_schema(
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
