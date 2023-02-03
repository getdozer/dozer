use crate::dag::channels::SourceChannelForwarder;
use crate::dag::errors::ExecutionError;
use crate::dag::node::{OutputPortDef, OutputPortType, PortHandle, Source, SourceFactory};
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use crate::dag::tests::app::NoneContext;
use std::time::Duration;

pub(crate) const GENERATOR_SOURCE_OUTPUT_PORT: PortHandle = 100;

#[derive(Debug)]
pub(crate) struct GeneratorSourceFactory {
    count: u64,
    running: Arc<AtomicBool>,
    stateful: bool,
}

impl GeneratorSourceFactory {
    pub fn new(count: u64, barrier: Arc<AtomicBool>, stateful: bool) -> Self {
        Self {
            count,
            running: barrier,
            stateful,
        }
    }
}

impl SourceFactory<NoneContext> for GeneratorSourceFactory {
    fn get_output_schema(
        &self,
        _port: &PortHandle,
    ) -> Result<(Schema, NoneContext), ExecutionError> {
        Ok((
            Schema::empty()
                .field(
                    FieldDefinition::new(
                        "id".to_string(),
                        FieldType::String,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    true,
                )
                .field(
                    FieldDefinition::new(
                        "value".to_string(),
                        FieldType::String,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    false,
                )
                .clone(),
            NoneContext {},
        ))
    }

    fn get_output_ports(&self) -> Result<Vec<OutputPortDef>, ExecutionError> {
        Ok(vec![OutputPortDef::new(
            GENERATOR_SOURCE_OUTPUT_PORT,
            if self.stateful {
                OutputPortType::StatefulWithPrimaryKeyLookup {
                    retr_old_records_for_updates: true,
                    retr_old_records_for_deletes: true,
                }
            } else {
                OutputPortType::Stateless
            },
        )])
    }

    fn prepare(
        &self,
        _output_schemas: HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(GeneratorSource {
            count: self.count,
            running: self.running.clone(),
        }))
    }
}

#[derive(Debug)]
pub(crate) struct GeneratorSource {
    count: u64,
    running: Arc<AtomicBool>,
}

impl Source for GeneratorSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        from_seq: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        let start = from_seq.unwrap_or((0, 0)).0;

        for n in start + 1..(start + self.count + 1) {
            fw.send(
                n,
                0,
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::String(format!("key_{n}")),
                            Field::String(format!("value_{n}")),
                        ],
                        None,
                    ),
                },
                GENERATOR_SOURCE_OUTPUT_PORT,
            )?;
        }

        loop {
            if !self.running.load(Ordering::Relaxed) {
                break;
            }
            thread::sleep(Duration::from_millis(500));
        }

        Ok(())
    }
}

pub(crate) const DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1: PortHandle = 1000;
pub(crate) const DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2: PortHandle = 2000;

#[derive(Debug)]
pub(crate) struct DualPortGeneratorSourceFactory {
    count: u64,
    running: Arc<AtomicBool>,
    stateful: bool,
}

impl DualPortGeneratorSourceFactory {
    pub fn new(count: u64, barrier: Arc<AtomicBool>, stateful: bool) -> Self {
        Self {
            count,
            running: barrier,
            stateful,
        }
    }
}

impl SourceFactory<NoneContext> for DualPortGeneratorSourceFactory {
    fn get_output_schema(
        &self,
        _port: &PortHandle,
    ) -> Result<(Schema, NoneContext), ExecutionError> {
        Ok((
            Schema::empty()
                .field(
                    FieldDefinition::new(
                        "id".to_string(),
                        FieldType::String,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    true,
                )
                .field(
                    FieldDefinition::new(
                        "value".to_string(),
                        FieldType::String,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    false,
                )
                .clone(),
            NoneContext {},
        ))
    }

    fn get_output_ports(&self) -> Result<Vec<OutputPortDef>, ExecutionError> {
        Ok(vec![
            OutputPortDef::new(
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1,
                if self.stateful {
                    OutputPortType::StatefulWithPrimaryKeyLookup {
                        retr_old_records_for_updates: true,
                        retr_old_records_for_deletes: true,
                    }
                } else {
                    OutputPortType::Stateless
                },
            ),
            OutputPortDef::new(
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2,
                if self.stateful {
                    OutputPortType::StatefulWithPrimaryKeyLookup {
                        retr_old_records_for_updates: true,
                        retr_old_records_for_deletes: true,
                    }
                } else {
                    OutputPortType::Stateless
                },
            ),
        ])
    }

    fn prepare(
        &self,
        _output_schemas: HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(DualPortGeneratorSource {
            count: self.count,
            running: self.running.clone(),
        }))
    }
}

#[derive(Debug)]
pub(crate) struct DualPortGeneratorSource {
    count: u64,
    running: Arc<AtomicBool>,
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
                            Field::String(format!("key_{n}")),
                            Field::String(format!("value_{n}")),
                        ],
                        None,
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
                            Field::String(format!("key_{n}")),
                            Field::String(format!("value_{n}")),
                        ],
                        None,
                    ),
                },
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2,
            )?;
        }
        loop {
            if !self.running.load(Ordering::Relaxed) {
                break;
            }
            thread::sleep(Duration::from_millis(500));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct NoPkGeneratorSourceFactory {
    count: u64,
    running: Arc<AtomicBool>,
    stateful: bool,
}

impl NoPkGeneratorSourceFactory {
    pub fn new(count: u64, barrier: Arc<AtomicBool>, stateful: bool) -> Self {
        Self {
            count,
            running: barrier,
            stateful,
        }
    }
}

impl SourceFactory<NoneContext> for NoPkGeneratorSourceFactory {
    fn get_output_schema(
        &self,
        _port: &PortHandle,
    ) -> Result<(Schema, NoneContext), ExecutionError> {
        Ok((
            Schema::empty()
                .field(
                    FieldDefinition::new(
                        "id".to_string(),
                        FieldType::String,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    true,
                )
                .field(
                    FieldDefinition::new(
                        "value".to_string(),
                        FieldType::String,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    false,
                )
                .clone(),
            NoneContext {},
        ))
    }

    fn get_output_ports(&self) -> Result<Vec<OutputPortDef>, ExecutionError> {
        Ok(vec![OutputPortDef::new(
            GENERATOR_SOURCE_OUTPUT_PORT,
            if self.stateful {
                OutputPortType::AutogenRowKeyLookup
            } else {
                OutputPortType::Stateless
            },
        )])
    }

    fn prepare(
        &self,
        _output_schemas: HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(NoPkGeneratorSource {
            count: self.count,
            running: self.running.clone(),
        }))
    }
}

#[derive(Debug)]
pub(crate) struct NoPkGeneratorSource {
    count: u64,
    running: Arc<AtomicBool>,
}

impl Source for NoPkGeneratorSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        from_seq: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        let start = from_seq.unwrap_or((0, 0)).0;

        for n in start + 1..(start + self.count + 1) {
            fw.send(
                n,
                0,
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::String(format!("key_{n}")),
                            Field::String(format!("value_{n}")),
                        ],
                        None,
                    ),
                },
                GENERATOR_SOURCE_OUTPUT_PORT,
            )?;
        }

        loop {
            if !self.running.load(Ordering::Relaxed) {
                break;
            }
            thread::sleep(Duration::from_millis(500));
        }

        Ok(())
    }
}
