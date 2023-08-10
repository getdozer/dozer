use crate::executor::{DagExecutor, ExecutorOptions};
use crate::node::{
    OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory, Source, SourceFactory,
};
use crate::processor_record::ProcessorRecordStore;
use crate::{Dag, Endpoint, DEFAULT_PORT_HANDLE};

use crate::tests::dag_base_run::NoopProcessorFactory;
use crate::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::tests::sources::{GeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT};

use dozer_log::tokio;
use dozer_types::errors::internal::BoxedError;
use dozer_types::node::NodeHandle;
use dozer_types::types::{FieldDefinition, FieldType, Schema, SourceDefinition};
use tempdir::TempDir;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::tests::app::NoneContext;

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
    fn get_output_schema(&self, _port: &PortHandle) -> Result<(Schema, NoneContext), BoxedError> {
        Ok((
            Schema::default()
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

    fn get_output_port_name(&self, _port: &PortHandle) -> String {
        "error".to_string()
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
        if self.panic {
            panic!("Generated error");
        } else {
            Err("Generated Error".to_string().into())
        }
    }
}

#[tokio::test]
#[should_panic]
async fn test_create_src_err() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(Some(1), 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Box::new(CreateErrSourceFactory::new(false)),
    );
    dag.add_processor(proc_handle.clone(), Box::new(NoopProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Box::new(CountingSinkFactory::new(count, latch)),
    );

    dag.connect(
        Endpoint::new(source_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
    )
    .unwrap();

    dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    )
    .unwrap();

    let temp_dir = TempDir::new("test_create_src_err").unwrap();
    DagExecutor::new(
        dag,
        temp_dir.path().to_str().unwrap().to_string(),
        ExecutorOptions::default(),
    )
    .await
    .unwrap()
    .start(Arc::new(AtomicBool::new(true)))
    .unwrap()
    .join()
    .unwrap();
}

#[tokio::test]
#[should_panic]
async fn test_create_src_panic() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(Some(1), 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Box::new(CreateErrSourceFactory::new(true)),
    );
    dag.add_processor(proc_handle.clone(), Box::new(NoopProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Box::new(CountingSinkFactory::new(count, latch)),
    );

    dag.connect(
        Endpoint::new(source_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
    )
    .unwrap();

    dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    )
    .unwrap();

    let temp_dir = TempDir::new("test_create_src_panic").unwrap();
    DagExecutor::new(
        dag,
        temp_dir.path().to_str().unwrap().to_string(),
        ExecutorOptions::default(),
    )
    .await
    .unwrap()
    .start(Arc::new(AtomicBool::new(true)))
    .unwrap()
    .join()
    .unwrap();
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
    fn type_name(&self) -> String {
        "CreateErr".to_owned()
    }

    fn get_output_schema(
        &self,
        _port: &PortHandle,
        _input_schemas: &HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(Schema, NoneContext), BoxedError> {
        Ok((
            Schema::default()
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

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _record_store: &ProcessorRecordStore,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        if self.panic {
            panic!("Generated error");
        } else {
            Err("Generated Error".to_string().into())
        }
    }

    fn id(&self) -> String {
        "CreateErr".to_owned()
    }
}

#[tokio::test]
#[should_panic]
async fn test_create_proc_err() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(Some(1), 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Box::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_processor(
        proc_handle.clone(),
        Box::new(CreateErrProcessorFactory::new(false)),
    );
    dag.add_sink(
        sink_handle.clone(),
        Box::new(CountingSinkFactory::new(count, latch)),
    );

    dag.connect(
        Endpoint::new(source_handle, GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
    )
    .unwrap();

    dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    )
    .unwrap();

    let temp_dir = TempDir::new("test_create_proc_err").unwrap();
    DagExecutor::new(
        dag,
        temp_dir.path().to_str().unwrap().to_string(),
        ExecutorOptions::default(),
    )
    .await
    .unwrap()
    .start(Arc::new(AtomicBool::new(true)))
    .unwrap()
    .join()
    .unwrap();
}

#[tokio::test]
#[should_panic]
async fn test_create_proc_panic() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(Some(1), 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Box::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_processor(
        proc_handle.clone(),
        Box::new(CreateErrProcessorFactory::new(true)),
    );
    dag.add_sink(
        sink_handle.clone(),
        Box::new(CountingSinkFactory::new(count, latch)),
    );

    dag.connect(
        Endpoint::new(source_handle, GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
    )
    .unwrap();

    dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    )
    .unwrap();

    let temp_dir = TempDir::new("test_create_proc_panic").unwrap();
    DagExecutor::new(
        dag,
        temp_dir.path().to_str().unwrap().to_string(),
        ExecutorOptions::default(),
    )
    .await
    .unwrap()
    .start(Arc::new(AtomicBool::new(true)))
    .unwrap()
    .join()
    .unwrap();
}
