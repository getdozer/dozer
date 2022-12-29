use crate::dag::channels::SourceChannelForwarder;
use crate::dag::errors::ExecutionError;
use crate::dag::node::{OutputPortDef, OutputPortDefOptions, PortHandle, Source, SourceFactory};
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};

use std::collections::HashMap;
use std::sync::{Arc, Barrier};

pub(crate) const GENERATOR_SOURCE_OUTPUT_PORT: PortHandle = 100;

#[derive(Debug)]
pub(crate) struct GeneratorSourceFactory {
    count: u64,
    barrier: Arc<Barrier>,
    stateful: bool,
}

impl GeneratorSourceFactory {
    pub fn new(count: u64, barrier: Arc<Barrier>, stateful: bool) -> Self {
        Self {
            count,
            barrier,
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
            )
            .field(
                FieldDefinition::new("value".to_string(), FieldType::String, false),
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
            barrier: self.barrier.clone(),
        }))
    }
}

#[derive(Debug)]
pub(crate) struct GeneratorSource {
    count: u64,
    barrier: Arc<Barrier>,
}

impl Source for GeneratorSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        from_seq: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        let start = from_seq.unwrap().0;

        for n in start + 1..(start + self.count + 1) {
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
        self.barrier.wait();
        Ok(())
    }
}

pub(crate) const DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1: PortHandle = 1000;
pub(crate) const DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2: PortHandle = 2000;

#[derive(Debug)]
pub(crate) struct DualPortGeneratorSourceFactory {
    count: u64,
    barrier: Arc<Barrier>,
    stateful: bool,
}

impl DualPortGeneratorSourceFactory {
    pub fn new(count: u64, barrier: Arc<Barrier>, stateful: bool) -> Self {
        Self {
            count,
            barrier,
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
            )
            .field(
                FieldDefinition::new("value".to_string(), FieldType::String, false),
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
            barrier: self.barrier.clone(),
        }))
    }
}

#[derive(Debug)]
pub(crate) struct DualPortGeneratorSource {
    count: u64,
    barrier: Arc<Barrier>,
}

impl Source for DualPortGeneratorSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<(u64, u64)>,
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
        self.barrier.wait();
        Ok(())
    }
}
