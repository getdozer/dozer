use crate::channels::SourceChannelForwarder;
use crate::errors::ExecutionError;
use crate::node::{OutputPortDef, OutputPortType, PortHandle, Source, SourceFactory};
use crate::DEFAULT_PORT_HANDLE;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use crate::tests::app::NoneContext;
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

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            GENERATOR_SOURCE_OUTPUT_PORT,
            if self.stateful {
                OutputPortType::StatefulWithPrimaryKeyLookup
            } else {
                OutputPortType::Stateless
            },
        )]
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
    fn can_start_from(&self, _last_checkpoint: (u64, u64)) -> Result<bool, ExecutionError> {
        Ok(true)
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        last_checkpoint: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        let start = last_checkpoint.unwrap_or((0, 0)).0;

        for n in start + 1..(start + self.count + 1) {
            fw.send(
                IngestionMessage::new_op(
                    n,
                    0,
                    Operation::Insert {
                        new: Record::new(
                            None,
                            vec![
                                Field::String(format!("key_{n}")),
                                Field::String(format!("value_{n}")),
                            ],
                        ),
                    },
                ),
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

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![
            OutputPortDef::new(
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1,
                if self.stateful {
                    OutputPortType::StatefulWithPrimaryKeyLookup
                } else {
                    OutputPortType::Stateless
                },
            ),
            OutputPortDef::new(
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2,
                if self.stateful {
                    OutputPortType::StatefulWithPrimaryKeyLookup
                } else {
                    OutputPortType::Stateless
                },
            ),
        ]
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
    fn can_start_from(&self, _last_checkpoint: (u64, u64)) -> Result<bool, ExecutionError> {
        Ok(false)
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _last_checkpoint: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        for n in 1..(self.count + 1) {
            fw.send(
                IngestionMessage::new_op(
                    n,
                    0,
                    Operation::Insert {
                        new: Record::new(
                            None,
                            vec![
                                Field::String(format!("key_{n}")),
                                Field::String(format!("value_{n}")),
                            ],
                        ),
                    },
                ),
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1,
            )?;
            fw.send(
                IngestionMessage::new_op(
                    n,
                    0,
                    Operation::Insert {
                        new: Record::new(
                            None,
                            vec![
                                Field::String(format!("key_{n}")),
                                Field::String(format!("value_{n}")),
                            ],
                        ),
                    },
                ),
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
pub struct ConnectivityTestSourceFactory;

impl SourceFactory<NoneContext> for ConnectivityTestSourceFactory {
    fn get_output_schema(
        &self,
        _port: &PortHandle,
    ) -> Result<(Schema, NoneContext), ExecutionError> {
        unimplemented!("This struct is for connectivity test, only output ports are defined")
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::Stateless,
        )]
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        unimplemented!("This struct is for connectivity test, only output ports are defined")
    }
}
