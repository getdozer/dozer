use crate::errors::ExecutionError;
use crate::node::{PortHandle, Sink, SinkFactory};
use crate::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Operation, Schema};

use dozer_types::log::debug;
use std::collections::HashMap;

use crate::tests::app::NoneContext;
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

impl SinkFactory<NoneContext> for CountingSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![COUNTING_SINK_INPUT_PORT]
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        Ok(Box::new(CountingSink {
            expected: self.expected,
            current: 0,
            running: self.running.clone(),
        }))
    }
}

#[derive(Debug)]
pub(crate) struct CountingSink {
    expected: u64,
    current: u64,
    running: Arc<AtomicBool>,
}
impl Sink for CountingSink {
    fn commit(&mut self) -> Result<(), ExecutionError> {
        // if self.current == self.expected {
        //     info!(
        //         "Received {} messages. Notifying sender to exit!",
        //         self.current
        //     );
        //     self.running.store(false, Ordering::Relaxed);
        // }
        Ok(())
    }

    fn process(&mut self, _from_port: PortHandle, _op: Operation) -> Result<(), ExecutionError> {
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

    fn on_source_snapshotting_done(
        &mut self,
        _connection_name: String,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct ConnectivityTestSinkFactory;

impl SinkFactory<NoneContext> for ConnectivityTestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(), BoxedError> {
        unimplemented!("This struct is for connectivity test, only input ports are defined")
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        unimplemented!("This struct is for connectivity test, only input ports are defined")
    }
}

#[derive(Debug)]
pub struct NoInputPortSinkFactory;

impl SinkFactory<NoneContext> for NoInputPortSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![]
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(), BoxedError> {
        unimplemented!("This struct is for connectivity test, only input ports are defined")
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        unimplemented!("This struct is for connectivity test, only input ports are defined")
    }
}
