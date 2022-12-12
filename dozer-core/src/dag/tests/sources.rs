use crate::dag::channels::SourceChannelForwarder;
use crate::dag::errors::ExecutionError;
use crate::dag::node::{OutputPortDef, OutputPortDefOptions, PortHandle, Source, SourceFactory};
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use fp_rust::sync::CountDownLatch;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) const GENERATOR_SOURCE_OUTPUT_PORT: PortHandle = 100;

pub(crate) struct GeneratorSourceFactory {
    count: u64,
    term_latch: Arc<CountDownLatch>,
    stateful: bool,
}

impl GeneratorSourceFactory {
    pub fn new(count: u64, term_latch: Arc<CountDownLatch>, stateful: bool) -> Self {
        Self {
            count,
            term_latch,
            stateful,
        }
    }
}

impl SourceFactory for GeneratorSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, ExecutionError> {
        Ok(Schema::empty()
            .field(
                FieldDefinition::new("id".to_string(), FieldType::String, false),
                true,
                true,
            )
            .field(
                FieldDefinition::new("value".to_string(), FieldType::String, false),
                true,
                false,
            )
            .clone())
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            GENERATOR_SOURCE_OUTPUT_PORT,
            OutputPortDefOptions::new(self.stateful, self.stateful, self.stateful),
        )]
    }
    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(GeneratorSource {
            count: self.count,
            term_latch: self.term_latch.clone(),
        }))
    }
}

pub(crate) struct GeneratorSource {
    count: u64,
    term_latch: Arc<CountDownLatch>,
}

impl Source for GeneratorSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<u64>,
    ) -> Result<(), ExecutionError> {
        for n in 1..(self.count + 1) {
            fw.send(
                n,
                0,
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::String(format!("key_{}", n)),
                            Field::String(format!("value_{}", n)),
                        ],
                    ),
                },
                GENERATOR_SOURCE_OUTPUT_PORT,
            )?;
        }
        self.term_latch.wait();
        Ok(())
    }
}

pub(crate) const DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1: PortHandle = 1000;
pub(crate) const DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2: PortHandle = 2000;

pub(crate) struct DualPortGeneratorSourceFactory {
    count: u64,
    term_latch: Arc<CountDownLatch>,
    stateful: bool,
}

impl DualPortGeneratorSourceFactory {
    pub fn new(count: u64, term_latch: Arc<CountDownLatch>, stateful: bool) -> Self {
        Self {
            count,
            term_latch,
            stateful,
        }
    }
}

impl SourceFactory for DualPortGeneratorSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, ExecutionError> {
        Ok(Schema::empty()
            .field(
                FieldDefinition::new("id".to_string(), FieldType::String, false),
                true,
                true,
            )
            .field(
                FieldDefinition::new("value".to_string(), FieldType::String, false),
                true,
                false,
            )
            .clone())
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![
            OutputPortDef::new(
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1,
                OutputPortDefOptions::new(self.stateful, self.stateful, self.stateful),
            ),
            OutputPortDef::new(
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2,
                OutputPortDefOptions::new(self.stateful, self.stateful, self.stateful),
            ),
        ]
    }
    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(DualPortGeneratorSource {
            count: self.count,
            term_latch: self.term_latch.clone(),
        }))
    }
}

pub(crate) struct DualPortGeneratorSource {
    count: u64,
    term_latch: Arc<CountDownLatch>,
}

impl Source for DualPortGeneratorSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<u64>,
    ) -> Result<(), ExecutionError> {
        for n in 1..(self.count + 1) {
            fw.send(
                n,
                0,
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::String(format!("key_{}", n)),
                            Field::String(format!("value_{}", n)),
                        ],
                    ),
                },
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1,
            )?;
            fw.send(
                n,
                0,
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::String(format!("key_{}", n)),
                            Field::String(format!("value_{}", n)),
                        ],
                    ),
                },
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2,
            )?;
        }
        self.term_latch.wait();
        Ok(())
    }
}
