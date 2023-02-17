use crate::channels::ProcessorChannelForwarder;
use crate::errors::ExecutionError;
use crate::executor::{DagExecutor, ExecutorOptions};
use crate::node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory};
use crate::record_store::RecordReader;
use crate::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::tests::sources::{
    GeneratorSourceFactory, NoPkGeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT,
};
use crate::{Dag, Endpoint};
use dozer_storage::lmdb_storage::{LmdbExclusiveTransaction, SharedTransaction};
use dozer_types::node::NodeHandle;
use dozer_types::types::{Field, Operation, Schema};

use std::collections::HashMap;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use crate::epoch::Epoch;
use crate::tests::app::NoneContext;
use tempdir::TempDir;

macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

pub(crate) const PASSTHROUGH_PROCESSOR_INPUT_PORT: PortHandle = 50;
pub(crate) const PASSTHROUGH_PROCESSOR_OUTPUT_PORT: PortHandle = 60;

#[derive(Debug)]
pub(crate) struct PassthroughProcessorFactory {}

impl PassthroughProcessorFactory {
    pub fn new() -> Self {
        Self {}
    }
}

impl ProcessorFactory<NoneContext> for PassthroughProcessorFactory {
    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(Schema, NoneContext), ExecutionError> {
        Ok(input_schemas
            .get(&PASSTHROUGH_PROCESSOR_INPUT_PORT)
            .unwrap()
            .clone())
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![PASSTHROUGH_PROCESSOR_INPUT_PORT]
    }
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            PASSTHROUGH_PROCESSOR_OUTPUT_PORT,
            OutputPortType::StatefulWithPrimaryKeyLookup {
                retr_old_records_for_deletes: true,
                retr_old_records_for_updates: true,
            },
        )]
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        Ok(Box::new(PassthroughProcessor {}))
    }
}

#[derive(Debug)]
pub(crate) struct PassthroughProcessor {}

impl Processor for PassthroughProcessor {
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
        _readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<(), ExecutionError> {
        fw.send(op, PASSTHROUGH_PROCESSOR_OUTPUT_PORT)
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

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        Ok(Box::new(RecordReaderProcessor { ctr: 1 }))
    }
}

#[derive(Debug)]
pub(crate) struct RecordReaderProcessor {
    ctr: u64,
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
        let v = readers
            .get(&RECORD_READER_PROCESSOR_INPUT_PORT)
            .unwrap()
            .get(
                Field::String(format!("key_{}", self.ctr))
                    .encode()
                    .as_slice(),
                1,
            )?;
        assert!(v.is_some());
        self.ctr += 1;

        fw.send(op, RECORD_READER_PROCESSOR_OUTPUT_PORT)
    }
}

#[test]
fn test_run_dag_record_reader() {
    const TOT: u64 = 10_000;

    let sync = Arc::new(AtomicBool::new(true));

    let src = GeneratorSourceFactory::new(TOT, sync.clone(), false);
    let passthrough = PassthroughProcessorFactory::new();
    let record_reader = RecordReaderProcessorFactory::new();
    let sink = CountingSinkFactory::new(TOT, sync);

    let mut dag = Dag::new();

    let source_id: NodeHandle = NodeHandle::new(None, 1.to_string());
    let passthrough_id: NodeHandle = NodeHandle::new(Some(1), 1.to_string());
    let record_reader_id: NodeHandle = NodeHandle::new(Some(1), 2.to_string());
    let sink_id: NodeHandle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(source_id.clone(), Arc::new(src));
    dag.add_processor(passthrough_id.clone(), Arc::new(passthrough));
    dag.add_processor(record_reader_id.clone(), Arc::new(record_reader));
    dag.add_sink(sink_id.clone(), Arc::new(sink));

    assert!(dag
        .connect(
            Endpoint::new(source_id, GENERATOR_SOURCE_OUTPUT_PORT),
            Endpoint::new(passthrough_id.clone(), PASSTHROUGH_PROCESSOR_INPUT_PORT),
        )
        .is_ok());

    assert!(dag
        .connect(
            Endpoint::new(passthrough_id, PASSTHROUGH_PROCESSOR_OUTPUT_PORT),
            Endpoint::new(record_reader_id.clone(), RECORD_READER_PROCESSOR_INPUT_PORT),
        )
        .is_ok());

    assert!(dag
        .connect(
            Endpoint::new(record_reader_id, RECORD_READER_PROCESSOR_OUTPUT_PORT),
            Endpoint::new(sink_id, COUNTING_SINK_INPUT_PORT),
        )
        .is_ok());

    let options = ExecutorOptions {
        commit_sz: 1000,
        commit_time_threshold: Duration::from_millis(5),
        ..Default::default()
    };

    let tmp_dir = chk!(TempDir::new("test"));
    DagExecutor::new(&dag, tmp_dir.path().to_path_buf(), options)
        .unwrap()
        .start(Arc::new(AtomicBool::new(true)))
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn test_run_dag_record_reader_from_src() {
    const TOT: u64 = 1_000;

    let sync = Arc::new(AtomicBool::new(true));

    let src = GeneratorSourceFactory::new(TOT, sync.clone(), true);
    let record_reader = RecordReaderProcessorFactory::new();
    let sink = CountingSinkFactory::new(TOT, sync);

    let mut dag = Dag::new();

    let source_id: NodeHandle = NodeHandle::new(None, 1.to_string());
    let record_reader_id: NodeHandle = NodeHandle::new(Some(1), 1.to_string());
    let sink_id: NodeHandle = NodeHandle::new(Some(1), 2.to_string());

    dag.add_source(source_id.clone(), Arc::new(src));
    dag.add_processor(record_reader_id.clone(), Arc::new(record_reader));
    dag.add_sink(sink_id.clone(), Arc::new(sink));

    assert!(dag
        .connect(
            Endpoint::new(source_id, GENERATOR_SOURCE_OUTPUT_PORT),
            Endpoint::new(record_reader_id.clone(), RECORD_READER_PROCESSOR_INPUT_PORT),
        )
        .is_ok());

    assert!(dag
        .connect(
            Endpoint::new(record_reader_id, RECORD_READER_PROCESSOR_OUTPUT_PORT),
            Endpoint::new(sink_id, COUNTING_SINK_INPUT_PORT),
        )
        .is_ok());

    let tmp_dir = chk!(TempDir::new("test"));
    DagExecutor::new(
        &dag,
        tmp_dir.path().to_path_buf(),
        ExecutorOptions::default(),
    )
    .unwrap()
    .start(Arc::new(AtomicBool::new(true)))
    .unwrap()
    .join()
    .unwrap();
}

#[derive(Debug)]
pub(crate) struct NoPkRecordReaderProcessorFactory {}

impl NoPkRecordReaderProcessorFactory {
    pub fn new() -> Self {
        Self {}
    }
}

impl ProcessorFactory<NoneContext> for NoPkRecordReaderProcessorFactory {
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

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        Ok(Box::new(NoPkRecordReaderProcessor { ctr: 1 }))
    }
}

#[derive(Debug)]
pub(crate) struct NoPkRecordReaderProcessor {
    ctr: u64,
}

impl Processor for NoPkRecordReaderProcessor {
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
        let v = readers
            .get(&RECORD_READER_PROCESSOR_INPUT_PORT)
            .unwrap()
            .get(Field::UInt(self.ctr).encode().as_slice(), 1)?;
        assert!(v.is_some());
        self.ctr += 1;

        fw.send(op, RECORD_READER_PROCESSOR_OUTPUT_PORT)
    }
}

#[test]
fn test_run_dag_record_reader_from_rowkey_autogen_src() {
    const TOT: u64 = 1_000;

    let sync = Arc::new(AtomicBool::new(true));

    let src = NoPkGeneratorSourceFactory::new(TOT, sync.clone(), true);
    let record_reader = NoPkRecordReaderProcessorFactory::new();
    let sink = CountingSinkFactory::new(TOT, sync);

    let mut dag = Dag::new();

    let source_id: NodeHandle = NodeHandle::new(None, 1.to_string());
    let record_reader_id: NodeHandle = NodeHandle::new(Some(1), 1.to_string());
    let sink_id: NodeHandle = NodeHandle::new(Some(1), 2.to_string());

    dag.add_source(source_id.clone(), Arc::new(src));
    dag.add_processor(record_reader_id.clone(), Arc::new(record_reader));
    dag.add_sink(sink_id.clone(), Arc::new(sink));

    assert!(dag
        .connect(
            Endpoint::new(source_id, GENERATOR_SOURCE_OUTPUT_PORT),
            Endpoint::new(record_reader_id.clone(), RECORD_READER_PROCESSOR_INPUT_PORT),
        )
        .is_ok());

    assert!(dag
        .connect(
            Endpoint::new(record_reader_id, RECORD_READER_PROCESSOR_OUTPUT_PORT),
            Endpoint::new(sink_id, COUNTING_SINK_INPUT_PORT),
        )
        .is_ok());

    let tmp_dir = chk!(TempDir::new("test"));
    DagExecutor::new(
        &dag,
        tmp_dir.path().to_path_buf(),
        ExecutorOptions::default(),
    )
    .unwrap()
    .start(Arc::new(AtomicBool::new(true)))
    .unwrap()
    .join()
    .unwrap();
}
