use crate::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::errors::ExecutionError;

use dozer_types::epoch::Epoch;
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};

pub type PortHandle = u16;

#[derive(Debug, Clone, Copy)]
pub enum OutputPortType {
    Stateless,
    StatefulWithPrimaryKeyLookup,
}

impl Display for OutputPortType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputPortType::Stateless => f.write_str("Stateless"),
            OutputPortType::StatefulWithPrimaryKeyLookup { .. } => {
                f.write_str("StatefulWithPrimaryKeyLookup")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutputPortDef {
    pub handle: PortHandle,
    pub typ: OutputPortType,
}

impl OutputPortDef {
    pub fn new(handle: PortHandle, typ: OutputPortType) -> Self {
        Self { handle, typ }
    }
}

pub trait SourceFactory<T>: Send + Sync + Debug {
    fn get_output_schema(&self, port: &PortHandle) -> Result<(Schema, T), ExecutionError>;
    fn get_output_ports(&self) -> Vec<OutputPortDef>;
    fn build(
        &self,
        output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError>;
}

pub trait Source: Send + Sync + Debug {
    /// Checks if the source can start from the given checkpoint.
    /// If this function returns false, the executor will start the source from the beginning.
    fn can_start_from(&self, last_checkpoint: (u64, u64)) -> Result<bool, ExecutionError>;
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        last_checkpoint: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError>;
}

pub trait ProcessorFactory<T>: Send + Sync + Debug {
    fn get_output_schema(
        &self,
        output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, (Schema, T)>,
    ) -> Result<(Schema, T), ExecutionError>;
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn get_output_ports(&self) -> Vec<OutputPortDef>;
    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError>;
}

pub trait Processor: Send + Sync + Debug {
    fn commit(&self, epoch_details: &Epoch) -> Result<(), ExecutionError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), ExecutionError>;
}

pub trait SinkFactory<T>: Send + Sync + Debug {
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn prepare(
        &self,
        input_schemas: HashMap<PortHandle, (Schema, T)>,
    ) -> Result<(), ExecutionError>;
    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, ExecutionError>;
}

pub trait Sink: Send + Sync + Debug {
    fn commit(&mut self) -> Result<(), ExecutionError>;
    fn process(&mut self, from_port: PortHandle, op: Operation) -> Result<(), ExecutionError>;

    fn on_source_snapshotting_done(&mut self) -> Result<(), ExecutionError>;
}
