use crate::dag::epoch::Epoch;
use crate::dag::errors::ExecutionError;
use crate::dag::node::{PortHandle, Sink, SinkFactory};
use crate::dag::record_store::RecordReader;
use crate::storage::lmdb_storage::{LmdbEnvironmentManager, SharedTransaction};
use dozer_types::types::{Operation, Schema};

use log::info;
use std::collections::HashMap;
use std::sync::{Arc, Barrier};

pub(crate) const COUNTING_SINK_INPUT_PORT: PortHandle = 90;

pub(crate) struct CountingSinkFactory {
    expected: u64,
    barrier: Arc<Barrier>,
}

impl CountingSinkFactory {
    pub fn new(expected: u64, barrier: Arc<Barrier>) -> Self {
        Self { expected, barrier }
    }
}

impl SinkFactory for CountingSinkFactory {
    fn set_input_schema(
        &self,
        _input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![COUNTING_SINK_INPUT_PORT]
    }
    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, ExecutionError> {
        Ok(Box::new(CountingSink {
            expected: self.expected,
            current: 0,
            barrier: self.barrier.clone(),
        }))
    }
}

pub(crate) struct CountingSink {
    expected: u64,
    current: u64,
    barrier: Arc<Barrier>,
}
impl Sink for CountingSink {
    fn init(&mut self, _state: &mut LmdbEnvironmentManager) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn commit(
        &mut self,
        _epoch_details: &Epoch,
        _tx: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _op: Operation,
        _state: &SharedTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        self.current += 1;
        if self.current == self.expected {
            info!(
                "Received {} messages. Notifying sender to exit!",
                self.current
            );
            self.barrier.wait();
        }
        Ok(())
    }
}
