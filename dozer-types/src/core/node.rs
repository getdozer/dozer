use crate::core::channels::{ChannelManager, ProcessorChannelForwarder, SourceChannelForwarder};
use crate::errors::execution::ExecutionError;
use crate::types::{Operation, Schema};
use rocksdb::DB;
use std::collections::HashMap;
use std::sync::Arc;

pub type NodeHandle = String;
pub type PortHandle = u16;

pub trait ProcessorFactory: Send + Sync {
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn get_output_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn Processor>;
}

pub trait Processor {
    fn update_schema(
        &mut self,
        output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError>;
    fn init(&mut self, db: Arc<DB>) -> Result<(), ExecutionError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &dyn ProcessorChannelForwarder,
        db: &DB,
    ) -> Result<(), ExecutionError>;
}

pub trait SourceFactory: Send + Sync {
    fn get_output_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn Source>;
}

pub trait Source {
    fn get_output_schema(&self, port: PortHandle) -> Schema;
    fn start(
        &self,
        fw: &dyn SourceChannelForwarder,
        cm: &dyn ChannelManager,
        db: Arc<DB>,
        from_seq: Option<u64>,
    ) -> Result<(), ExecutionError>;
}

pub trait SinkFactory: Send + Sync {
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn Sink>;
}

pub trait Sink {
    fn update_schema(
        &mut self,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError>;
    fn init(&mut self, db: Arc<DB>) -> Result<(), ExecutionError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        seq: u64,
        op: Operation,
        db: &DB,
    ) -> Result<(), ExecutionError>;
}
