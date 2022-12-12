use crate::dag::errors::ExecutionError;
use crate::dag::node::{PortHandle, Sink, SinkFactory};
use crate::dag::record_store::RecordReader;
use crate::storage::common::{Environment, RwTransaction};
use dozer_types::types::{Operation, Schema};
use fp_rust::sync::CountDownLatch;
use log::info;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) const COUNTING_SINK_INPUT_PORT: PortHandle = 90;

pub(crate) struct CountingSinkFactory {
    expected: u64,
    term_latch: Arc<CountDownLatch>,
}

impl CountingSinkFactory {
    pub fn new(expected: u64, term_latch: Arc<CountDownLatch>) -> Self {
        Self {
            expected,
            term_latch,
        }
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
            term_latch: self.term_latch.clone(),
        }))
    }
}

pub(crate) struct CountingSink {
    expected: u64,
    current: u64,
    term_latch: Arc<CountDownLatch>,
}
impl Sink for CountingSink {
    fn init(&mut self, _state: &mut dyn Environment) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn commit(&self, _tx: &mut dyn RwTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _txid: u64,
        _seq_in_tx: u64,
        _op: Operation,
        _state: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        self.current += 1;
        if self.current == self.expected {
            info!(
                "Received {} messages. Notifying sender to exit!",
                self.current
            );
            self.term_latch.countdown();
        }
        Ok(())
    }
}
