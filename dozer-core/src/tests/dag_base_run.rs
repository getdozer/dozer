use crate::channels::ProcessorChannelForwarder;
use crate::dag_schemas::DagSchemas;
use crate::errors::ExecutionError;
use crate::executor::{DagExecutor, ExecutorOptions};
use crate::node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory};
use crate::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::tests::sources::{
    DualPortGeneratorSourceFactory, GeneratorSourceFactory,
    DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1, DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2,
    GENERATOR_SOURCE_OUTPUT_PORT,
};
use crate::{Dag, Endpoint, DEFAULT_PORT_HANDLE};
use dozer_types::epoch::Epoch;
use dozer_types::errors::internal::BoxedError;
use dozer_types::node::NodeHandle;
use dozer_types::types::{Operation, Schema};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::tests::app::NoneContext;

#[derive(Debug)]
pub(crate) struct NoopProcessorFactory {}

impl ProcessorFactory<NoneContext> for NoopProcessorFactory {
    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(Schema, NoneContext), BoxedError> {
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

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        Ok(Box::new(NoopProcessor {}))
    }
}

#[derive(Debug)]
pub(crate) struct NoopProcessor {}

impl Processor for NoopProcessor {
    fn commit(&self, _epoch_details: &Epoch) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), ExecutionError> {
        fw.send(op, DEFAULT_PORT_HANDLE)
    }
}

#[test]
fn test_run_dag() {
    let count: u64 = 1_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(Some(1), 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Arc::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_processor(proc_handle.clone(), Arc::new(NoopProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Arc::new(CountingSinkFactory::new(count, latch)),
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

    DagExecutor::new(dag, ExecutorOptions::default())
        .unwrap()
        .start(Arc::new(AtomicBool::new(true)))
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn test_run_dag_and_stop() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(None, 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Arc::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_processor(proc_handle.clone(), Arc::new(NoopProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Arc::new(CountingSinkFactory::new(count, latch)),
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

    let running = Arc::new(AtomicBool::new(true));
    let join_handle = DagExecutor::new(dag.clone(), ExecutorOptions::default())
        .unwrap()
        .start(running.clone())
        .unwrap();

    thread::sleep(Duration::from_millis(1000));
    running.store(false, Ordering::SeqCst);
    join_handle.join().unwrap();

    DagSchemas::new(dag).unwrap();
}

#[derive(Debug)]
pub(crate) struct NoopJoinProcessorFactory {}

pub const NOOP_JOIN_LEFT_INPUT_PORT: u16 = 1;
pub const NOOP_JOIN_RIGHT_INPUT_PORT: u16 = 2;

impl ProcessorFactory<NoneContext> for NoopJoinProcessorFactory {
    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(Schema, NoneContext), BoxedError> {
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

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        Ok(Box::new(NoopJoinProcessor {}))
    }
}

#[derive(Debug)]
pub(crate) struct NoopJoinProcessor {}

impl Processor for NoopJoinProcessor {
    fn commit(&self, _epoch_details: &Epoch) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), ExecutionError> {
        fw.send(op, DEFAULT_PORT_HANDLE)
    }
}

#[test]
fn test_run_dag_2_sources_stateless() {
    let count: u64 = 50_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source1_handle = NodeHandle::new(None, 1.to_string());
    let source2_handle = NodeHandle::new(None, 2.to_string());

    let proc_handle = NodeHandle::new(Some(1), 1.to_string());
    let sink_handle = NodeHandle::new(Some(1), 2.to_string());

    dag.add_source(
        source1_handle.clone(),
        Arc::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_source(
        source2_handle.clone(),
        Arc::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_processor(proc_handle.clone(), Arc::new(NoopJoinProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Arc::new(CountingSinkFactory::new(count * 2, latch)),
    );

    dag.connect(
        Endpoint::new(source1_handle, GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), 1),
    )
    .unwrap();

    dag.connect(
        Endpoint::new(source2_handle, GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), 2),
    )
    .unwrap();

    dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    )
    .unwrap();

    DagExecutor::new(dag, ExecutorOptions::default())
        .unwrap()
        .start(Arc::new(AtomicBool::new(true)))
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn test_run_dag_2_sources_stateful() {
    let count: u64 = 50_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source1_handle = NodeHandle::new(None, 1.to_string());
    let source2_handle = NodeHandle::new(None, 2.to_string());

    let proc_handle = NodeHandle::new(Some(1), 1.to_string());
    let sink_handle = NodeHandle::new(Some(1), 2.to_string());

    dag.add_source(
        source1_handle.clone(),
        Arc::new(GeneratorSourceFactory::new(count, latch.clone(), true)),
    );
    dag.add_source(
        source2_handle.clone(),
        Arc::new(GeneratorSourceFactory::new(count, latch.clone(), true)),
    );
    dag.add_processor(proc_handle.clone(), Arc::new(NoopJoinProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Arc::new(CountingSinkFactory::new(count * 2, latch)),
    );

    dag.connect(
        Endpoint::new(source1_handle, GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), 1),
    )
    .unwrap();

    dag.connect(
        Endpoint::new(source2_handle, GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_handle.clone(), 2),
    )
    .unwrap();

    dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    )
    .unwrap();

    DagExecutor::new(dag, ExecutorOptions::default())
        .unwrap()
        .start(Arc::new(AtomicBool::new(true)))
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn test_run_dag_1_source_2_ports_stateless() {
    let count: u64 = 50_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(None, 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 1.to_string());
    let sink_handle = NodeHandle::new(Some(1), 2.to_string());

    dag.add_source(
        source_handle.clone(),
        Arc::new(DualPortGeneratorSourceFactory::new(
            count,
            latch.clone(),
            false,
        )),
    );
    dag.add_processor(proc_handle.clone(), Arc::new(NoopJoinProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Arc::new(CountingSinkFactory::new(count * 2, latch)),
    );

    dag.connect(
        Endpoint::new(
            source_handle.clone(),
            DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1,
        ),
        Endpoint::new(proc_handle.clone(), 1),
    )
    .unwrap();

    dag.connect(
        Endpoint::new(source_handle, DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2),
        Endpoint::new(proc_handle.clone(), 2),
    )
    .unwrap();

    dag.connect(
        Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    )
    .unwrap();

    DagExecutor::new(dag, ExecutorOptions::default())
        .unwrap()
        .start(Arc::new(AtomicBool::new(true)))
        .unwrap()
        .join()
        .unwrap();
}
