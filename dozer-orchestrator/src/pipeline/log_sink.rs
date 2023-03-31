use std::collections::HashMap;

use dozer_core::{
    epoch::Epoch,
    errors::ExecutionError,
    executor::ExecutorOperation,
    node::{PortHandle, Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::types::{Operation, Schema};

pub struct LogSinkSettings;

#[derive(Debug)]
pub struct LogSinkFactory;

impl LogSinkFactory {
    pub fn new() -> Self {
        todo!()
    }
}

impl<T> SinkFactory<T> for LogSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn prepare(
        &self,
        input_schemas: HashMap<PortHandle, (Schema, T)>,
    ) -> Result<(), ExecutionError> {
        todo!("Assert there's only one input schema. Store the schema.")
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, ExecutionError> {
        todo!("Create log file. Write schema to log file.")
    }
}

#[derive(Debug)]
pub struct LogSink;

impl Sink for LogSink {
    fn process(&mut self, from_port: PortHandle, op: Operation) -> Result<(), ExecutionError> {
        let executor_operation = ExecutorOperation::Op { op };
        todo!("Write operation to log file.")
    }

    fn commit(&mut self) -> Result<(), ExecutionError> {
        let executor_operation = ExecutorOperation::Commit {
            epoch: Epoch::new(0, Default::default()),
        };
        todo!("Write commit to log file.")
    }

    fn on_source_snapshotting_done(&mut self) -> Result<(), ExecutionError> {
        let executor_operation = ExecutorOperation::SnapshottingDone {};
        todo!("Write snapshotting done to log file.")
    }
}
