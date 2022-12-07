use crate::chk;
use crate::dag::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::dag::{Dag, Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use crate::dag::errors::ExecutionError;
use crate::dag::executor::{DagExecutor, ExecutorOptions};
use crate::dag::node::{
    NodeHandle, OutputPortDef, OutputPortDefOptions, PortHandle, Processor, ProcessorFactory, Sink,
    SinkFactory, Source, SourceFactory,
};
use crate::dag::record_store::RecordReader;
use crate::dag::tests::common::init_log4rs;
use crate::dag::tests::dag_base_run::NoopProcessorFactory;
use crate::dag::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::dag::tests::sources::{GeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT};
use crate::storage::common::{Environment, RwTransaction};
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use fp_rust::sync::CountDownLatch;

use std::collections::HashMap;
use std::sync::Arc;

use tempdir::TempDir;

struct CreateErrSourceFactory {
    panic: bool,
}

impl CreateErrSourceFactory {
    pub fn new(panic: bool) -> Self {
        Self { panic }
    }
}

impl SourceFactory for CreateErrSourceFactory {
    fn get_output_schema(&self, port: &PortHandle) -> Result<Schema, ExecutionError> {
        Ok(Schema::empty()
            .field(
                FieldDefinition::new("id".to_string(), FieldType::Int, false),
                true,
                true,
            )
            .clone())
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortDefOptions::default(),
        )]
    }

    fn build(
        &self,
        output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        if self.panic {
            panic!("Generated error");
        } else {
            Err(ExecutionError::InvalidOperation(
                "Generated Error".to_string(),
            ))
        }
    }
}

#[test]
fn test_create_src_err() {
    init_log4rs();

    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(CountDownLatch::new(1));

    let source_handle = NodeHandle::new(Some(1), 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_node(
        NodeType::Source(Arc::new(CreateErrSourceFactory::new(false))),
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
        Endpoint::new(source_handle.clone(), DEFAULT_PORT_HANDLE),
        Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
    ));

    chk!(dag.connect(
        Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle.clone(), COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default()
    ));

    chk!(executor.start());
    assert!(executor.join().is_err());
}

#[test]
fn test_create_src_panic() {
    init_log4rs();

    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(CountDownLatch::new(1));

    let source_handle = NodeHandle::new(Some(1), 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_node(
        NodeType::Source(Arc::new(CreateErrSourceFactory::new(true))),
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
        Endpoint::new(source_handle.clone(), DEFAULT_PORT_HANDLE),
        Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
    ));

    chk!(dag.connect(
        Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle.clone(), COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default()
    ));

    chk!(executor.start());
    assert!(executor.join().is_err());
}

struct CreateErrProcessorFactory {
    panic: bool,
}

impl CreateErrProcessorFactory {
    pub fn new(panic: bool) -> Self {
        Self { panic }
    }
}

impl ProcessorFactory for CreateErrProcessorFactory {
    fn get_output_schema(
        &self,
        port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        Ok(Schema::empty()
            .field(
                FieldDefinition::new("id".to_string(), FieldType::Int, false),
                true,
                true,
            )
            .clone())
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

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        if self.panic {
            panic!("Generated error");
        } else {
            Err(ExecutionError::InvalidOperation(
                "Generated Error".to_string(),
            ))
        }
    }
}

#[test]
fn test_create_proc_err() {
    init_log4rs();

    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(CountDownLatch::new(1));

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
        NodeType::Processor(Arc::new(CreateErrProcessorFactory::new(false))),
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
        Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle.clone(), COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default()
    ));

    chk!(executor.start());
    assert!(executor.join().is_err());
}

#[test]
fn test_create_proc_panic() {
    init_log4rs();

    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(CountDownLatch::new(1));

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
        NodeType::Processor(Arc::new(CreateErrProcessorFactory::new(true))),
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
        Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle.clone(), COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default()
    ));

    chk!(executor.start());
    assert!(executor.join().is_err());
}
