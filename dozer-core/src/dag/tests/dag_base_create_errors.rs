use crate::chk;

use crate::dag::errors::ExecutionError;
use crate::dag::executor::{DagExecutor, ExecutorOptions};
use crate::dag::node::{
    NodeHandle, OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory, Source,
    SourceFactory,
};
use crate::dag::{Dag, Endpoint, DEFAULT_PORT_HANDLE};

use crate::dag::tests::dag_base_run::NoopProcessorFactory;
use crate::dag::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::dag::tests::sources::{GeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT};

use dozer_types::types::{FieldDefinition, FieldType, Schema, SourceDefinition};

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::dag::tests::app::NoneContext;
use tempdir::TempDir;

#[derive(Debug)]
struct CreateErrSourceFactory {
    panic: bool,
}

impl CreateErrSourceFactory {
    pub fn new(panic: bool) -> Self {
        Self { panic }
    }
}

impl SourceFactory<NoneContext> for CreateErrSourceFactory {
    fn get_output_schema(
        &self,
        _port: &PortHandle,
    ) -> Result<(Schema, NoneContext), ExecutionError> {
        Ok((
            Schema::empty()
                .field(
                    FieldDefinition::new(
                        "id".to_string(),
                        FieldType::Int,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    true,
                )
                .clone(),
            NoneContext {},
        ))
    }

    fn get_output_ports(&self) -> Result<Vec<OutputPortDef>, ExecutionError> {
        Ok(vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::Stateless,
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
        _output_schemas: HashMap<PortHandle, Schema>,
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
#[should_panic]
fn test_create_src_err() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(Some(1), 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Arc::new(CreateErrSourceFactory::new(false)),
    );
    dag.add_processor(proc_handle.clone(), Arc::new(NoopProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Arc::new(CountingSinkFactory::new(count, latch)),
    );

    chk!(dag.connect(
        Endpoint::new(source_handle, DEFAULT_PORT_HANDLE),
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
    assert!(executor.join().is_err());
}

#[test]
#[should_panic]
fn test_create_src_panic() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(Some(1), 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Arc::new(CreateErrSourceFactory::new(true)),
    );
    dag.add_processor(proc_handle.clone(), Arc::new(NoopProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Arc::new(CountingSinkFactory::new(count, latch)),
    );

    chk!(dag.connect(
        Endpoint::new(source_handle, DEFAULT_PORT_HANDLE),
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
    assert!(executor.join().is_err());
}

#[derive(Debug)]
struct CreateErrProcessorFactory {
    panic: bool,
}

impl CreateErrProcessorFactory {
    pub fn new(panic: bool) -> Self {
        Self { panic }
    }
}

impl ProcessorFactory<NoneContext> for CreateErrProcessorFactory {
    fn get_output_schema(
        &self,
        _port: &PortHandle,
        _input_schemas: &HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(Schema, NoneContext), ExecutionError> {
        Ok((
            Schema::empty()
                .field(
                    FieldDefinition::new(
                        "id".to_string(),
                        FieldType::Int,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    true,
                )
                .clone(),
            NoneContext {},
        ))
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
#[should_panic]
fn test_create_proc_err() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(Some(1), 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Arc::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_processor(
        proc_handle.clone(),
        Arc::new(CreateErrProcessorFactory::new(false)),
    );
    dag.add_sink(
        sink_handle.clone(),
        Arc::new(CountingSinkFactory::new(count, latch)),
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
    assert!(executor.join().is_err());
}

#[test]
#[should_panic]
fn test_create_proc_panic() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(Some(1), 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Arc::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_processor(
        proc_handle.clone(),
        Arc::new(CreateErrProcessorFactory::new(true)),
    );
    dag.add_sink(
        sink_handle.clone(),
        Arc::new(CountingSinkFactory::new(count, latch)),
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
    assert!(executor.join().is_err());
}
