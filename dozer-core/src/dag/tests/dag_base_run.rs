use crate::chk;
use crate::dag::channels::ProcessorChannelForwarder;
use crate::dag::dag::{Dag, Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use crate::dag::errors::ExecutionError;
use crate::dag::executor::{DagExecutor, ExecutorOptions};
use crate::dag::node::{
    OutputPortDef, OutputPortDefOptions, PortHandle, Processor, ProcessorFactory,
};
use crate::dag::record_store::RecordReader;
use crate::dag::tests::common::init_log4rs;
use crate::dag::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::dag::tests::sources::{
    GeneratorSource, GeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT,
};
use crate::storage::common::{Environment, RwTransaction};
use dozer_types::types::{Operation, Schema};
use fp_rust::sync::CountDownLatch;
use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempdir::TempDir;

pub(crate) struct NoopProcessorFactory {}

impl ProcessorFactory for NoopProcessorFactory {
    fn get_output_schema(
        &self,
        output_port: &PortHandle,
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
            OutputPortDefOptions::default(),
        )]
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(NoopProcessor {})
    }
}

pub(crate) struct NoopProcessor {}

impl Processor for NoopProcessor {
    fn init(&mut self, state: &mut dyn Environment) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn commit(&self, tx: &mut dyn RwTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        tx: &mut dyn RwTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        fw.send(op, DEFAULT_PORT_HANDLE)
    }
}

#[test]
fn test_run_dag() {
    init_log4rs();

    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(CountDownLatch::new(1));

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            count,
            latch.clone(),
            false,
        ))),
        "source".to_string(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopProcessorFactory {})),
        "proc".to_string(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(CountingSinkFactory::new(count, latch.clone()))),
        "sink".to_string(),
    );

    chk!(dag.connect(
        Endpoint::new("source".to_string(), GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new("proc".to_string(), DEFAULT_PORT_HANDLE),
    ));

    chk!(dag.connect(
        Endpoint::new("proc".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("sink".to_string(), COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        &tmp_dir.path(),
        ExecutorOptions::default()
    ));

    chk!(executor.start());
    assert!(executor.join().is_ok());
}

pub(crate) struct NoopJoinProcessorFactory {}

impl ProcessorFactory for NoopJoinProcessorFactory {
    fn get_output_schema(
        &self,
        output_port: &PortHandle,
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
            OutputPortDefOptions::default(),
        )]
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(NoopJoinProcessor {})
    }
}

pub(crate) struct NoopJoinProcessor {}

impl Processor for NoopJoinProcessor {
    fn init(&mut self, state: &mut dyn Environment) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn commit(&self, tx: &mut dyn RwTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        tx: &mut dyn RwTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        fw.send(op, DEFAULT_PORT_HANDLE)
    }
}

#[test]
fn test_run_dag_2_sources_stateless() {
    init_log4rs();

    let count: u64 = 50_000;

    let mut dag = Dag::new();
    let latch = Arc::new(CountDownLatch::new(1));

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            count,
            latch.clone(),
            false,
        ))),
        "source1".to_string(),
    );
    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            count,
            latch.clone(),
            false,
        ))),
        "source2".to_string(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopJoinProcessorFactory {})),
        "proc".to_string(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(CountingSinkFactory::new(count * 2, latch.clone()))),
        "sink".to_string(),
    );

    chk!(dag.connect(
        Endpoint::new("source1".to_string(), GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new("proc".to_string(), 1),
    ));

    chk!(dag.connect(
        Endpoint::new("source2".to_string(), GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new("proc".to_string(), 2),
    ));

    chk!(dag.connect(
        Endpoint::new("proc".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("sink".to_string(), COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        &tmp_dir.path(),
        ExecutorOptions::default()
    ));

    chk!(executor.start());
    assert!(executor.join().is_ok());
}

#[test]
fn test_run_dag_2_sources_stateful() {
    init_log4rs();

    let count: u64 = 50_000;

    let mut dag = Dag::new();
    let latch = Arc::new(CountDownLatch::new(1));

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            count,
            latch.clone(),
            true,
        ))),
        "source1".to_string(),
    );
    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(
            count,
            latch.clone(),
            true,
        ))),
        "source2".to_string(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopJoinProcessorFactory {})),
        "proc".to_string(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(CountingSinkFactory::new(count * 2, latch.clone()))),
        "sink".to_string(),
    );

    chk!(dag.connect(
        Endpoint::new("source1".to_string(), GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new("proc".to_string(), 1),
    ));

    chk!(dag.connect(
        Endpoint::new("source2".to_string(), GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new("proc".to_string(), 2),
    ));

    chk!(dag.connect(
        Endpoint::new("proc".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("sink".to_string(), COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        &tmp_dir.path(),
        ExecutorOptions::default()
    ));

    chk!(executor.start());
    assert!(executor.join().is_ok());
}
