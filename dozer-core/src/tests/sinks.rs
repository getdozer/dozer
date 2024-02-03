use crate::epoch::Epoch;
use crate::node::{PortHandle, Sink, SinkFactory};
use crate::DEFAULT_PORT_HANDLE;
use dozer_log::storage::Queue;
use dozer_recordstore::ProcessorRecordStore;
use dozer_types::errors::internal::BoxedError;
use dozer_types::node::OpIdentifier;
use dozer_types::types::{OperationWithId, Schema};

use dozer_types::log::debug;
use std::collections::HashMap;

use dozer_types::tonic::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(crate) const COUNTING_SINK_INPUT_PORT: PortHandle = 90;

#[derive(Debug)]
pub(crate) struct CountingSinkFactory {
    expected: u64,
    running: Arc<AtomicBool>,
}

impl CountingSinkFactory {
    pub fn new(expected: u64, barrier: Arc<AtomicBool>) -> Self {
        Self {
            expected,
            running: barrier,
        }
    }
}

#[async_trait]
impl SinkFactory for CountingSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![COUNTING_SINK_INPUT_PORT]
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        Ok(())
    }

    async fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        Ok(Box::new(CountingSink {
            expected: self.expected,
            current: 0,
            running: self.running.clone(),
        }))
    }

    fn type_name(&self) -> String {
        "counting".to_string()
    }
}

#[derive(Debug)]
pub(crate) struct CountingSink {
    expected: u64,
    current: u64,
    running: Arc<AtomicBool>,
}
impl Sink for CountingSink {
    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        // if self.current == self.expected {
        //     info!(
        //         "Received {} messages. Notifying sender to exit!",
        //         self.current
        //     );
        //     self.running.store(false, Ordering::Relaxed);
        // }
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _record_store: &ProcessorRecordStore,
        _op: OperationWithId,
    ) -> Result<(), BoxedError> {
        self.current += 1;
        if self.current == self.expected {
            debug!(
                "Received {} messages. Notifying sender to exit!",
                self.current
            );
            self.running.store(false, Ordering::Relaxed);
        }
        Ok(())
    }

    fn persist(&mut self, _epoch: &Epoch, _queue: &Queue) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_started(
        &mut self,
        _connection_name: String,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_done(
        &mut self,
        _connection_name: String,
        _id: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn set_source_state(&mut self, _source_state: &[u8]) -> Result<(), BoxedError> {
        Ok(())
    }

    fn get_source_state(&mut self) -> Result<Option<Vec<u8>>, BoxedError> {
        Ok(None)
    }

    fn get_latest_op_id(&mut self) -> Result<Option<OpIdentifier>, BoxedError> {
        Ok(None)
    }
}

#[derive(Debug)]
pub struct ConnectivityTestSinkFactory;

#[async_trait]
impl SinkFactory for ConnectivityTestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        unimplemented!("This struct is for connectivity test, only input ports are defined")
    }

    async fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        unimplemented!("This struct is for connectivity test, only input ports are defined")
    }

    fn type_name(&self) -> String {
        "connectivity_test".to_string()
    }
}

#[derive(Debug)]
pub struct NoInputPortSinkFactory;

#[async_trait]
impl SinkFactory for NoInputPortSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![]
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        unimplemented!("This struct is for connectivity test, only input ports are defined")
    }

    async fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        unimplemented!("This struct is for connectivity test, only input ports are defined")
    }

    fn type_name(&self) -> String {
        "no_input_port".to_string()
    }
}
