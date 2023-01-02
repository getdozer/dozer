use crate::chk;
use crate::dag::channels::ProcessorChannelForwarder;
use crate::dag::dag::{Dag, Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use crate::dag::errors::ExecutionError;
use crate::dag::executor::{DagExecutor, ExecutorOptions};
use crate::dag::node::{
    NodeHandle, OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory,
};
use crate::dag::record_store::RecordReader;
use crate::dag::tests::common::init_log4rs;
use crate::dag::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::dag::tests::sources::{
    DualPortGeneratorSourceFactory, GeneratorSourceFactory,
    DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1, DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2,
    GENERATOR_SOURCE_OUTPUT_PORT,
};
use crate::storage::lmdb_storage::{LmdbEnvironmentManager, SharedTransaction};
use dozer_types::types::{Operation, Schema};

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::dag::dag_metadata::{Consistency, DagMetadataManager};
use crate::dag::epoch::Epoch;
use tempdir::TempDir;

#[derive(Debug)]
pub(crate) struct NoopProcessorFactory {}

impl ProcessorFactory for NoopProcessorFactory {
    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        Ok(input_schemas.get(&DEFAULT_PORT_HANDLE).unwrap().clone())
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::Stateless,
        )]
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        Ok(Box::new(NoopProcessor {}))
    }
}

#[derive(Debug)]
pub(crate) struct NoopProcessor {}

impl Processor for NoopProcessor {
    fn init(&mut self, _state: &mut LmdbEnvironmentManager) -> Result<(), ExecutionError> {
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
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        fw.send(op, DEFAULT_PORT_HANDLE)
    }
}

#[test]
fn test_run_dag() {
    // dozer_tracing::init_telemetry(false).unwrap();

    let count: u64 = 1_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(Some(1), 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            count,
            latch.clone(),
            false,
        ))),
        source_handle.clone(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopProcessorFactory {})),
        proc_handle.clone(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(CountingSinkFactory::new(count, latch))),
        sink_handle.clone(),
    );

    chk!(dag.connect(
        Endpoint::new(source_handle, GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
    ));

    chk!(dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default(),
        Arc::new(AtomicBool::new(true))
    ));

    chk!(executor.start());
    assert!(executor.join().is_ok());
}

#[test]
fn test_run_dag_and_stop() {
    init_log4rs();

    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(None, 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            count,
            latch.clone(),
            false,
        ))),
        source_handle.clone(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopProcessorFactory {})),
        proc_handle.clone(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(CountingSinkFactory::new(count, latch))),
        sink_handle.clone(),
    );

    chk!(dag.connect(
        Endpoint::new(source_handle.clone(), GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
    ));

    chk!(dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default(),
        Arc::new(AtomicBool::new(true))
    ));

    chk!(executor.start());

    thread::sleep(Duration::from_millis(1000));
    executor.stop();
    assert!(executor.join().is_ok());

    let r = chk!(DagMetadataManager::new(&dag, tmp_dir.path()));
    let c = r.get_checkpoint_consistency();
    assert!(matches!(
        c.get(&source_handle).unwrap(),
        Consistency::FullyConsistent(_)
    ));
}

#[derive(Debug)]
pub(crate) struct NoopJoinProcessorFactory {}

pub const NOOP_JOIN_LEFT_INPUT_PORT: u16 = 1;
pub const NOOP_JOIN_RIGHT_INPUT_PORT: u16 = 2;

impl ProcessorFactory for NoopJoinProcessorFactory {
    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        Ok(input_schemas.get(&1).unwrap().clone())
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![1, 2]
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::Stateless,
        )]
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        Ok(Box::new(NoopJoinProcessor {}))
    }
}

#[derive(Debug)]
pub(crate) struct NoopJoinProcessor {}

impl Processor for NoopJoinProcessor {
    fn init(&mut self, _state: &mut LmdbEnvironmentManager) -> Result<(), ExecutionError> {
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
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        fw.send(op, DEFAULT_PORT_HANDLE)
    }
}

#[test]
fn test_run_dag_2_sources_stateless() {
    init_log4rs();

    let count: u64 = 50_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source1_handle = NodeHandle::new(None, 1.to_string());
    let source2_handle = NodeHandle::new(None, 2.to_string());

    let proc_handle = NodeHandle::new(Some(1), 1.to_string());
    let sink_handle = NodeHandle::new(Some(1), 2.to_string());

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            count,
            latch.clone(),
            false,
        ))),
        source1_handle.clone(),
    );
    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            count,
            latch.clone(),
            false,
        ))),
        source2_handle.clone(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopJoinProcessorFactory {})),
        proc_handle.clone(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(CountingSinkFactory::new(count * 2, latch))),
        sink_handle.clone(),
    );

    chk!(dag.connect(
        Endpoint::new(source1_handle, GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), 1),
    ));

    chk!(dag.connect(
        Endpoint::new(source2_handle, GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), 2),
    ));

    chk!(dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default(),
        Arc::new(AtomicBool::new(true))
    ));

    chk!(executor.start());
    assert!(executor.join().is_ok());
}

#[test]
fn test_run_dag_2_sources_stateful() {
    init_log4rs();

    let count: u64 = 50_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source1_handle = NodeHandle::new(None, 1.to_string());
    let source2_handle = NodeHandle::new(None, 2.to_string());

    let proc_handle = NodeHandle::new(Some(1), 1.to_string());
    let sink_handle = NodeHandle::new(Some(1), 2.to_string());

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            count,
            latch.clone(),
            true,
        ))),
        source1_handle.clone(),
    );
    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            count,
            latch.clone(),
            true,
        ))),
        source2_handle.clone(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopJoinProcessorFactory {})),
        proc_handle.clone(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(CountingSinkFactory::new(count * 2, latch))),
        sink_handle.clone(),
    );

    chk!(dag.connect(
        Endpoint::new(source1_handle, GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), 1),
    ));

    chk!(dag.connect(
        Endpoint::new(source2_handle, GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), 2),
    ));

    chk!(dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default(),
        Arc::new(AtomicBool::new(true))
    ));

    chk!(executor.start());
    assert!(executor.join().is_ok());
}

#[test]
fn test_run_dag_1_source_2_ports_stateless() {
    init_log4rs();

    let count: u64 = 50_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(None, 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 1.to_string());
    let sink_handle = NodeHandle::new(Some(1), 2.to_string());

    dag.add_node(
        NodeType::Source(Arc::new(DualPortGeneratorSourceFactory::new(
            count,
            latch.clone(),
            false,
        ))),
        source_handle.clone(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopJoinProcessorFactory {})),
        proc_handle.clone(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(CountingSinkFactory::new(count * 2, latch))),
        sink_handle.clone(),
    );

    chk!(dag.connect(
        Endpoint::new(
            source_handle.clone(),
            DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1
        ),
        Endpoint::new(proc_handle.clone(), 1),
    ));

    chk!(dag.connect(
        Endpoint::new(source_handle, DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2),
        Endpoint::new(proc_handle.clone(), 2),
    ));

    chk!(dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default(),
        Arc::new(AtomicBool::new(true))
    ));

    chk!(executor.start());
    let r = executor.join();
    assert!(r.is_ok());
}
