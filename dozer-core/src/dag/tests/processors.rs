use crate::dag::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::errors::ExecutionError;
use crate::dag::executor_local::DEFAULT_PORT_HANDLE;
use crate::dag::node::PortHandle;
use crate::dag::node::{Processor, ProcessorFactory, Sink, SinkFactory, Source, SourceFactory};
use crate::storage::common::{Database, Environment, RwTransaction};
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
    fn get_output_schema(&self, port: PortHandle) -> Option<Schema> {
        Some(
            Schema::empty()
                .field(
                    FieldDefinition::new(
                        format!("node_{}_port_{}", self.id, port),
                        FieldType::String,
                        false,
                    ),
                    false,
                    false,
                )
                .clone(),
        )
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<u64>,
    ) -> Result<(), ExecutionError> {
        for n in 0..1_000_000 {
            fw.send(
                n,
                Operation::Insert {
                    new: Record::new(None, vec![]),
                },
                DEFAULT_PORT_HANDLE,
            )?;
        }
        fw.terminate().unwrap();
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
    fn is_stateful(&self) -> bool {
        true
    }

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
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn init(&mut self, _state: Option<&mut dyn Environment>) -> Result<(), ExecutionError> {
        debug!("SINK {}: Initialising TestSink", self.id);
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _seq: u64,
        _op: Operation,
        _state: Option<&mut dyn RwTransaction>,
    ) -> Result<(), ExecutionError> {
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
    fn is_stateful(&self) -> bool {
        true
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }
    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.output_ports.clone()
    }
    fn build(&self) -> Box<dyn Processor> {
        Box::new(TestProcessor {
            id: self.id,
            ctr: 0,
            db: None,
        })
    }
}

pub struct TestProcessor {
    id: i32,
    ctr: u64,
    db: Option<Database>,
}

impl Processor for TestProcessor {
    fn update_schema(
        &mut self,
        output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
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

    fn init<'a>(&'_ mut self, tx: Option<&mut dyn Environment>) -> Result<(), ExecutionError> {
        debug!("PROC {}: Initialising TestProcessor", self.id);
        self.db = Some(tx.unwrap().open_database("test", false)?);
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        state: Option<&mut dyn RwTransaction>,
    ) -> Result<(), ExecutionError> {
        self.ctr += 1;

        let tx = state.unwrap();

        tx.put(
            self.db.as_ref().unwrap(),
            &self.ctr.to_ne_bytes(),
            &self.id.to_ne_bytes(),
        )?;
        let v = tx.get(self.db.as_ref().unwrap(), &self.ctr.to_ne_bytes())?;
        assert!(v.is_some());
        fw.send(op, DEFAULT_PORT_HANDLE)?;
        Ok(())
    }
}
