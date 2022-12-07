use crate::dag::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::errors::ExecutionError;
use crate::dag::record_store::RecordReader;
use crate::storage::common::{Environment, RwTransaction};
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

//pub type NodeHandle = String;
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)]
pub struct NodeHandle {
    ns: Option<u16>,
    id: u16,
}

impl NodeHandle {
    pub fn new(ns: Option<u16>, id: u16) -> Self {
        Self { ns, id }
    }
}

impl NodeHandle {
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut r = Vec::<u8>::with_capacity(5);
        match self.ns {
            Some(ns) => {
                r.push(1_u8);
                r.extend(ns.to_le_bytes());
            }
            None => r.push(0_u8),
        }
        r.extend(self.id.to_le_bytes());
        r
    }

    pub(crate) fn from_bytes(buffer: &[u8]) -> NodeHandle {
        match buffer[0] {
            1_u8 => {
                let ns = u16::from_le_bytes(buffer[1..3].try_into().unwrap());
                let id: u16 = u16::from_le_bytes(buffer[3..5].try_into().unwrap());
                NodeHandle::new(Some(ns), id)
            }
            _ => {
                let id: u16 = u16::from_le_bytes(buffer[1..3].try_into().unwrap());
                NodeHandle::new(None, id)
            }
        }
    }
}

impl Display for NodeHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ns_str = match self.ns {
            Some(ns) => ns.to_string(),
            None => "r".to_string(),
        };
        f.write_str(&*format!("{}_{}", ns_str, self.id))
    }
}

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

pub trait SourceFactory: Send + Sync {
    fn get_output_schema(&self, port: &PortHandle) -> Result<Schema, ExecutionError>;
    fn get_output_ports(&self) -> Vec<OutputPortDef>;
    fn build(&self, output_schemas: HashMap<PortHandle, Schema>) -> Box<dyn Source>;
}

pub trait Source {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        from_seq: Option<u64>,
    ) -> Result<(), ExecutionError>;
}

pub trait ProcessorFactory: Send + Sync {
    fn get_output_schema(
        &self,
        output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError>;
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn get_output_ports(&self) -> Vec<OutputPortDef>;
    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        output_schemas: HashMap<PortHandle, Schema>,
    ) -> Box<dyn Processor>;
}

pub trait Processor {
    fn init(&mut self, state: &mut dyn Environment) -> Result<(), ExecutionError>;
    fn commit(&self, tx: &mut dyn RwTransaction) -> Result<(), ExecutionError>;
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
    fn set_input_schema(
        &self,
        output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError>;
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn build(&self, input_schemas: HashMap<PortHandle, Schema>) -> Box<dyn Sink>;
}

pub trait Sink {
    fn init(&mut self, state: &mut dyn Environment) -> Result<(), ExecutionError>;
    fn commit(&self, tx: &mut dyn RwTransaction) -> Result<(), ExecutionError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        seq: u64,
        op: Operation,
        state: &mut dyn RwTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError>;
}
