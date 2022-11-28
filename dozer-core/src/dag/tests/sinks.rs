use crate::dag::errors::ExecutionError;
use crate::dag::node::{PortHandle, Sink, SinkFactory};
use crate::dag::record_store::RecordReader;
use crate::storage::common::{Environment, RwTransaction};
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;

pub(crate) const COUNTING_SINK_INPUT_PORT: PortHandle = 90;

pub(crate) struct CountingSinkFactory {
    expected: u64,
}

impl CountingSinkFactory {
    pub fn new(expected: u64) -> Self {
        Self { expected }
    }
}

impl SinkFactory for CountingSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![COUNTING_SINK_INPUT_PORT]
    }
    fn build(&self) -> Box<dyn Sink> {
        Box::new(CountingSink {
            expected: self.expected,
            current: 0,
        })
    }
}

pub(crate) struct CountingSink {
    expected: u64,
    current: u64,
}
impl Sink for CountingSink {
    fn update_schema(
        &mut self,
        _input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn init(&mut self, _state: &mut dyn Environment) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn commit(&self, _tx: &mut dyn RwTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _seq: u64,
        _op: Operation,
        _state: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}
