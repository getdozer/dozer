#![allow(non_snake_case)]
use crate::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::epoch::Epoch;
use crate::errors::ExecutionError;
use crate::executor::{DagExecutor, ExecutorOptions};
use crate::node::{
    NodeHandle, OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory, Source,
    SourceFactory,
};
use crate::record_store::RecordReader;
use crate::tests::app::NoneContext;
use crate::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::{Dag, Endpoint};
use dozer_storage::lmdb_storage::{LmdbExclusiveTransaction, SharedTransaction};
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempdir::TempDir;

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
        _from_seq: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        let mut txid: u64 = 0;

        for n in 0..self.count {
            fw.send(
                txid,
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
            txid += 1;
        }

        for n in 0..self.count {
            fw.send(
                txid,
                0,
                Operation::Update {
                    old: Record::new(
                        None,
                        vec![
                            Field::String(format!("key_{n}")),
                            Field::String(format!("value_{n}")),
                        ],
                        None,
                    ),
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
            txid += 1;
        }

        for n in 0..self.count {
            fw.send(
                txid,
                0,
                Operation::Delete {
                    old: Record::new(
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
            txid += 1;
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
pub(crate) struct RecordReaderProcessorFactory {}

impl RecordReaderProcessorFactory {
    pub fn new() -> Self {
        Self {}
    }
}

pub(crate) const RECORD_READER_PROCESSOR_INPUT_PORT: PortHandle = 70;
pub(crate) const RECORD_READER_PROCESSOR_OUTPUT_PORT: PortHandle = 80;

impl ProcessorFactory<NoneContext> for RecordReaderProcessorFactory {
    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(Schema, NoneContext), ExecutionError> {
        Ok(input_schemas
            .get(&RECORD_READER_PROCESSOR_INPUT_PORT)
            .unwrap()
            .clone())
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![RECORD_READER_PROCESSOR_INPUT_PORT]
    }
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            RECORD_READER_PROCESSOR_OUTPUT_PORT,
            OutputPortType::Stateless,
        )]
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, NoneContext)>,
        _output_schemas: HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        Ok(Box::new(RecordReaderProcessor { _ctr: 1 }))
    }
}

#[derive(Debug)]
pub(crate) struct RecordReaderProcessor {
    _ctr: u64,
}

impl Processor for RecordReaderProcessor {
    fn init(&mut self, _txn: &mut LmdbExclusiveTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn commit(
        &self,
        _epoch_details: &Epoch,
        _tx: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        _tx: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<(), ExecutionError> {
        match &op {
            Operation::Update { old, new: _ } => {
                let v = readers
                    .get(&RECORD_READER_PROCESSOR_INPUT_PORT)
                    .unwrap()
                    .get(old.values[0].encode().as_slice(), old.version.unwrap())?;
                assert!(v.is_some());
                fw.send(op, RECORD_READER_PROCESSOR_OUTPUT_PORT)
            }
            Operation::Delete { old } => {
                let v = readers
                    .get(&RECORD_READER_PROCESSOR_INPUT_PORT)
                    .unwrap()
                    .get(old.values[0].encode().as_slice(), old.version.unwrap())?;
                assert!(v.is_some());
                fw.send(op, RECORD_READER_PROCESSOR_OUTPUT_PORT)
            }
            _ => Ok(()),
        }
    }
}

#[test]
fn test_run_dag_record_reader_from_src() {
    const TOT: u64 = 30_000;

    let sync = Arc::new(AtomicBool::new(true));

    let src = GeneratorSourceFactory::new(TOT, sync.clone(), true);
    let record_reader = RecordReaderProcessorFactory::new();
    let sink = CountingSinkFactory::new(TOT * 2, sync);

    let mut dag = Dag::new();

    let SOURCE_ID: NodeHandle = NodeHandle::new(None, 1.to_string());
    let RECORD_READER_ID: NodeHandle = NodeHandle::new(Some(1), 1.to_string());
    let SINK_ID: NodeHandle = NodeHandle::new(Some(1), 2.to_string());

    dag.add_source(SOURCE_ID.clone(), Arc::new(src));
    dag.add_processor(RECORD_READER_ID.clone(), Arc::new(record_reader));
    dag.add_sink(SINK_ID.clone(), Arc::new(sink));

    assert!(dag
        .connect(
            Endpoint::new(SOURCE_ID, GENERATOR_SOURCE_OUTPUT_PORT),
            Endpoint::new(RECORD_READER_ID.clone(), RECORD_READER_PROCESSOR_INPUT_PORT),
        )
        .is_ok());

    assert!(dag
        .connect(
            Endpoint::new(RECORD_READER_ID, RECORD_READER_PROCESSOR_OUTPUT_PORT),
            Endpoint::new(SINK_ID, COUNTING_SINK_INPUT_PORT),
        )
        .is_ok());

    let tmp_dir = TempDir::new("test").unwrap();
    let options = ExecutorOptions::default();
    DagExecutor::new(&dag, tmp_dir.path().to_path_buf(), options)
        .unwrap()
        .start(Arc::new(AtomicBool::new(true)))
        .unwrap()
        .join()
        .unwrap();
}
