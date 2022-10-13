use crate::dag::dag::PortHandle;
use crate::dag::forwarder::{ChannelManager, ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::mt_executor::DEFAULT_PORT_HANDLE;
use crate::dag::node::{Processor, ProcessorFactory, Sink, SinkFactory, Source, SourceFactory};
use crate::state::StateStore;
use dozer_types::types::{FieldDefinition, FieldType, Operation, Record, Schema};
use log::debug;
use std::collections::HashMap;

/// Test Source
pub struct TestSourceFactory {
    id: i32,
    output_ports: Vec<PortHandle>,
}

impl TestSourceFactory {
    pub fn new(id: i32, output_ports: Vec<PortHandle>) -> Self {
        Self { id, output_ports }
    }
}

impl SourceFactory for TestSourceFactory {
    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.output_ports.clone()
    }
    fn build(&self) -> Box<dyn Source> {
        Box::new(TestSource { id: self.id })
    }
}

pub struct TestSource {
    id: i32,
}

impl Source for TestSource {
    fn get_output_schema(&self, port: PortHandle) -> Schema {
        Schema::empty()
            .field(
                FieldDefinition::new(
                    format!("node_{}_port_{}", self.id, port).to_string(),
                    FieldType::String,
                    false,
                ),
                false,
                false,
            )
            .clone()
    }

    fn start(
        &self,
        fw: &dyn SourceChannelForwarder,
        cm: &dyn ChannelManager,
        _state: &mut dyn StateStore,
        _from_seq: Option<u64>,
    ) -> anyhow::Result<()> {
        for n in 0..1_000_000 {
            fw.send(
                n,
                Operation::Insert {
                    new: Record::new(None, vec![]),
                },
                DEFAULT_PORT_HANDLE,
            )?;
        }
        cm.terminate().unwrap();
        Ok(())
    }
}

pub struct TestSinkFactory {
    id: i32,
    input_ports: Vec<PortHandle>,
}

impl TestSinkFactory {
    pub fn new(id: i32, input_ports: Vec<PortHandle>) -> Self {
        Self { id, input_ports }
    }
}

impl SinkFactory for TestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }
    fn build(&self) -> Box<dyn Sink> {
        Box::new(TestSink { id: self.id })
    }
}

pub struct TestSink {
    id: i32,
}

impl Sink for TestSink {
    fn update_schema(
        &mut self,
        _input_schemas: &HashMap<PortHandle, Schema>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn init(&mut self, _: &mut dyn StateStore) -> anyhow::Result<()> {
        debug!("SINK {}: Initialising TestSink", self.id);
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _seq: u64,
        _op: Operation,
        _state: &mut dyn StateStore,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

pub struct TestProcessorFactory {
    id: i32,
    input_ports: Vec<PortHandle>,
    output_ports: Vec<PortHandle>,
}

impl TestProcessorFactory {
    pub fn new(id: i32, input_ports: Vec<PortHandle>, output_ports: Vec<PortHandle>) -> Self {
        Self {
            id,
            input_ports,
            output_ports,
        }
    }
}

impl ProcessorFactory for TestProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }
    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.output_ports.clone()
    }
    fn build(&self) -> Box<dyn Processor> {
        Box::new(TestProcessor {
            state: None,
            id: self.id,
            ctr: 0,
        })
    }
}

pub struct TestProcessor {
    state: Option<Box<dyn StateStore>>,
    id: i32,
    ctr: u64,
}

impl Processor for TestProcessor {
    fn update_schema(
        &mut self,
        output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> anyhow::Result<Schema> {
        let mut sorted: Vec<PortHandle> = input_schemas.iter().map(|e| *e.0).collect();
        sorted.sort();
        let mut out_schema = Schema::empty();
        for i in sorted {
            let src_schema = input_schemas.get(&i).unwrap().clone();
            for f in src_schema.fields {
                out_schema.fields.push(f.clone());
            }
        }
        out_schema.fields.push(FieldDefinition::new(
            format!("node_{}_port_{}", self.id, output_port),
            FieldType::String,
            false,
        ));
        Ok(out_schema)
    }

    fn init<'a>(&'a mut self, _state_store: &mut dyn StateStore) -> anyhow::Result<()> {
        debug!("PROC {}: Initialising TestProcessor", self.id);
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &dyn ProcessorChannelForwarder,
        state_store: &mut dyn StateStore,
    ) -> anyhow::Result<()> {
        self.ctr += 1;
        state_store.put(&self.ctr.to_ne_bytes(), &self.id.to_ne_bytes())?;
        fw.send(op, DEFAULT_PORT_HANDLE)?;
        Ok(())
    }
}
