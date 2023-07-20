use crate::channels::{ProcessorChannelForwarder, SourceChannelForwarder};

use dozer_types::epoch::{Epoch, RefOperation};
use dozer_types::errors::internal::BoxedError;
use dozer_types::ref_types::RefSchema;
use dozer_types::types::Schema;
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
    fn get_output_schema(&self, port: &PortHandle) -> Result<(Schema, T), BoxedError>;
    fn get_output_ports(&self) -> Vec<OutputPortDef>;
    fn build(
        &self,
        output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, BoxedError>;
}

pub trait Source: Send + Sync + Debug {
    /// Checks if the source can start from the given checkpoint.
    /// If this function returns false, the executor will start the source from the beginning.
    fn can_start_from(&self, last_checkpoint: (u64, u64)) -> Result<bool, BoxedError>;
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        last_checkpoint: Option<(u64, u64)>,
    ) -> Result<(), BoxedError>;
}

pub trait ProcessorFactory<T>: Send + Sync + Debug {
    fn get_output_schema(
        &self,
        output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, (RefSchema, T)>,
    ) -> Result<(RefSchema, T), BoxedError>;
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn get_output_ports(&self) -> Vec<OutputPortDef>;
    fn build(
        &self,
        input_schemas: HashMap<PortHandle, RefSchema>,
        output_schemas: HashMap<PortHandle, RefSchema>,
    ) -> Result<Box<dyn Processor>, BoxedError>;
    fn type_name(&self) -> String;
    fn id(&self) -> String;
}

pub trait Processor: Send + Sync + Debug {
    fn commit(&self, epoch_details: &Epoch) -> Result<(), BoxedError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        op: RefOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError>;
}

pub trait SinkFactory<T>: Send + Sync + Debug {
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn prepare(&self, input_schemas: HashMap<PortHandle, (RefSchema, T)>)
        -> Result<(), BoxedError>;
    fn build(
        &self,
        input_schemas: HashMap<PortHandle, RefSchema>,
    ) -> Result<Box<dyn Sink>, BoxedError>;
}

pub trait Sink: Send + Sync + Debug {
    fn commit(&mut self, epoch_details: &Epoch) -> Result<(), BoxedError>;
    fn process(&mut self, from_port: PortHandle, op: RefOperation) -> Result<(), BoxedError>;

    fn on_source_snapshotting_done(&mut self, connection_name: String) -> Result<(), BoxedError>;
}
