use crate::channels::SourceChannelForwarder;
use crate::node::{OutputPortDef, OutputPortType, PortHandle, Source, SourceFactory, SourceState};
use crate::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::models::ingestion_types::IngestionMessage;
use dozer_types::node::OpIdentifier;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

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

impl SourceFactory for GeneratorSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, BoxedError> {
        Ok(Schema::default()
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
            .clone())
    }

    fn get_output_port_name(&self, _port: &PortHandle) -> String {
        "generator".to_string()
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
    ) -> Result<Box<dyn Source>, BoxedError> {
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
        last_checkpoint: SourceState,
    ) -> Result<(), BoxedError> {
        let start = last_checkpoint
            .values()
            .copied()
            .next()
            .flatten()
            .unwrap_or(OpIdentifier::new(0, 0))
            .txid;

        for n in start + 1..(start + self.count + 1) {
            fw.send(
                IngestionMessage::OperationEvent {
                    table_index: 0,
                    op: Operation::Insert {
                        new: Record::new(vec![
                            Field::String(format!("key_{n}")),
                            Field::String(format!("value_{n}")),
                        ]),
                    },
                    id: Some(OpIdentifier::new(n, 0)),
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

impl SourceFactory for DualPortGeneratorSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, BoxedError> {
        Ok(Schema::default()
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
            .clone())
    }

    fn get_output_port_name(&self, port: &PortHandle) -> String {
        match *port {
            DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1 => "generator1".to_string(),
            DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2 => "generator2".to_string(),
            _ => panic!("Unknown port"),
        }
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
    ) -> Result<Box<dyn Source>, BoxedError> {
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
        _last_checkpoint: SourceState,
    ) -> Result<(), BoxedError> {
        for n in 1..(self.count + 1) {
            fw.send(
                IngestionMessage::OperationEvent {
                    table_index: 0,
                    op: Operation::Insert {
                        new: Record::new(vec![
                            Field::String(format!("key_{n}")),
                            Field::String(format!("value_{n}")),
                        ]),
                    },
                    id: Some(OpIdentifier::new(n, 0)),
                },
                DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1,
            )?;
            fw.send(
                IngestionMessage::OperationEvent {
                    table_index: 0,
                    op: Operation::Insert {
                        new: Record::new(vec![
                            Field::String(format!("key_{n}")),
                            Field::String(format!("value_{n}")),
                        ]),
                    },
                    id: Some(OpIdentifier::new(n, 0)),
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
pub struct ConnectivityTestSourceFactory;

impl SourceFactory for ConnectivityTestSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, BoxedError> {
        unimplemented!("This struct is for connectivity test, only output ports are defined")
    }

    fn get_output_port_name(&self, _port: &PortHandle) -> String {
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
    ) -> Result<Box<dyn Source>, BoxedError> {
        unimplemented!("This struct is for connectivity test, only output ports are defined")
    }
}
