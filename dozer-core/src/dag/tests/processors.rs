use std::collections::HashMap;
use std::iter::Map;
use std::ops::Deref;

use anyhow::{anyhow, Context};

use dozer_types::types::{FieldDefinition, FieldType, Operation, OperationEvent, Record, Schema};

use crate::dag::dag::{NodeHandle, PortHandle};
use crate::dag::forwarder::{ChannelManager, ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::mt_executor::DefaultPortHandle;
use crate::dag::node::{NextStep, Processor, ProcessorFactory, Sink, SinkFactory, Source, SourceFactory};
use crate::dag::node::NextStep::Continue;
use crate::state::StateStore;

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
    fn get_output_schema(&self, port: PortHandle) -> anyhow::Result<Schema> {
        Ok(Schema::empty().field(
            FieldDefinition::new(format!("node_{}_port_{}", self.id, port).to_string(), FieldType::String, false),
            false, false,
        ).clone())
    }
    fn build(&self) -> Box<dyn Source> {
        Box::new(TestSource { id: self.id })
    }
}

pub struct TestSource {
    id: i32,
}

impl Source for TestSource {
    fn start(&self, fw: &dyn SourceChannelForwarder, cm: &dyn ChannelManager, state: &mut dyn StateStore, from_seq: Option<u64>) -> anyhow::Result<()> {
        for n in 0..1_000_000 {
            //  println!("SRC {}: Message {} received", self.id, n);
            fw.send(
                OperationEvent::new(
                    n,
                    Operation::Insert {
                        new: Record::new(None, vec![]),
                    },
                ),
                DefaultPortHandle,
            )
                .unwrap();
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
    fn init(&mut self, _: &mut dyn StateStore, input_schemas: HashMap<PortHandle, Schema>) -> anyhow::Result<()> {
        println!("SINK {}: Initialising TestSink", self.id);
        Ok(())
    }

    fn process(
        &self,
        _from_port: PortHandle,
        _op: OperationEvent,
        _state: &mut dyn StateStore,
    ) -> anyhow::Result<NextStep> {
        //    println!("SINK {}: Message {} received", self.id, _op.seq_no);
        Ok(Continue)
    }
}

pub struct TestProcessorFactory {
    id: i32,
    input_ports: Vec<PortHandle>,
    output_ports: Vec<PortHandle>,
}

impl TestProcessorFactory {
    pub fn new(id: i32, input_ports: Vec<PortHandle>, output_ports: Vec<PortHandle>) -> Self {
        Self { id, input_ports, output_ports }
    }
}

impl ProcessorFactory for TestProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }
    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.output_ports.clone()
    }
    fn get_output_schema(&self, output_port: PortHandle, input_schemas: HashMap<PortHandle, Schema>) -> anyhow::Result<Schema> {
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
            format!("node_{}_port_{}", self.id, output_port), FieldType::String, false,
        ));
        Ok(out_schema)
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(TestProcessor { state: None, id: self.id, ctr: 0 })
    }
}

pub struct TestProcessor {
    state: Option<Box<dyn StateStore>>,
    id: i32,
    ctr: u64,
}


impl Processor for TestProcessor {
    fn init<'a>(&'a mut self, state_store: &mut dyn StateStore, input_schemas: HashMap<PortHandle, Schema>) -> anyhow::Result<()> {
        println!("PROC {}: Initialising TestProcessor", self.id);
        //   self.state = Some(state_manager.init_state_store("pippo".to_string()).unwrap());
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &dyn ProcessorChannelForwarder,
        state_store: &mut dyn StateStore,
    ) -> anyhow::Result<NextStep> {

        //   println!("PROC {}: Message {} received", self.id, op.seq_no);
        self.ctr += 1;
        state_store.put(&self.ctr.to_ne_bytes(), &self.id.to_ne_bytes());
        fw.send(op, DefaultPortHandle)?;
        Ok(Continue)
    }
}

