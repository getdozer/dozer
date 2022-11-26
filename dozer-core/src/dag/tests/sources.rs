use crate::dag::channels::SourceChannelForwarder;
use crate::dag::errors::ExecutionError;
use crate::dag::node::{OutputPortDef, OutputPortDefOptions, PortHandle, Source, SourceFactory};
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use std::ops::Deref;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

pub(crate) const GENERATOR_SOURCE_OUTPUT_PORT: PortHandle = 100;

pub(crate) type FnOpGen = dyn Fn(u64) -> Operation + Send + Sync;

pub(crate) struct GeneratorSourceFactory {
    count: u64,
    sleep: Duration,
    sync: Arc<Barrier>,
    stateful_port: bool,
    schema: Schema,
    gen: &'static FnOpGen,
}

impl GeneratorSourceFactory {
    pub fn new(
        count: u64,
        sleep: Duration,
        sync: Arc<Barrier>,
        stateful_port: bool,
        schema: Schema,
        gen: &'static FnOpGen,
    ) -> Self {
        Self {
            count,
            sleep,
            sync,
            stateful_port,
            schema,
            gen,
        }
    }
}

impl SourceFactory for GeneratorSourceFactory {
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            GENERATOR_SOURCE_OUTPUT_PORT,
            OutputPortDefOptions::new(self.stateful_port, self.stateful_port, self.stateful_port),
        )]
    }
    fn build(&self) -> Box<dyn Source> {
        Box::new(GeneratorSource {
            count: self.count,
            sleep: self.sleep,
            sync: self.sync.clone(),
            schema: self.schema.clone(),
            gen: self.gen,
        })
    }
}

pub(crate) struct GeneratorSource {
    count: u64,
    sleep: Duration,
    sync: Arc<Barrier>,
    schema: Schema,
    gen: &'static FnOpGen,
}

impl Source for GeneratorSource {
    fn get_output_schema(&self, _port: PortHandle) -> Option<Schema> {
        Some(self.schema.clone())
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<u64>,
    ) -> Result<(), ExecutionError> {
        for n in 0..self.count {
            fw.send(n, (self.gen)(n), GENERATOR_SOURCE_OUTPUT_PORT)?;
            if !self.sleep.is_zero() {
                thread::sleep(self.sleep);
            }
        }
        self.sync.wait();
        Ok(())
    }
}
