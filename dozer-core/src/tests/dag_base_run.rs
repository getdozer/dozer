use crate::channels::ProcessorChannelForwarder;
use crate::checkpoint::create_checkpoint_for_test;
use crate::epoch::Epoch;
use crate::executor::DagExecutor;
use crate::node::{PortHandle, Processor, ProcessorFactory};
use crate::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::tests::sources::{
    DualPortGeneratorSourceFactory, GeneratorSourceFactory,
    DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_1, DUAL_PORT_GENERATOR_SOURCE_OUTPUT_PORT_2,
    GENERATOR_SOURCE_OUTPUT_PORT,
};
use crate::{Dag, Endpoint, DEFAULT_PORT_HANDLE};
use dozer_log::storage::Object;
use dozer_log::tokio::sync::oneshot;
use dozer_recordstore::{ProcessorRecordStore, ProcessorRecordStoreDeserializer};
use dozer_types::errors::internal::BoxedError;
use dozer_types::node::NodeHandle;
use dozer_types::tonic::async_trait;
use dozer_types::types::{OperationWithId, Schema};

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use super::{create_test_runtime, run_dag};

#[derive(Debug)]
pub(crate) struct NoopProcessorFactory {}

#[async_trait]
impl ProcessorFactory for NoopProcessorFactory {
    fn type_name(&self) -> String {
        "Noop".to_owned()
    }

    async fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        Ok(input_schemas.get(&DEFAULT_PORT_HANDLE).unwrap().clone())
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _checkpoint_data: Option<Vec<u8>>,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        Ok(Box::new(NoopProcessor {}))
    }

    fn id(&self) -> String {
        "Noop".to_owned()
    }
}

#[derive(Debug)]
pub(crate) struct NoopProcessor {}

impl Processor for NoopProcessor {
    fn commit(&self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: OperationWithId,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        fw.send(op, DEFAULT_PORT_HANDLE);
        Ok(())
    }

    fn serialize(&mut self, _object: Object) -> Result<(), BoxedError> {
        Ok(())
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
        Box::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_processor(proc_handle.clone(), Box::new(NoopProcessorFactory {}));
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

    run_dag(dag).unwrap();
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
        Box::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_processor(proc_handle.clone(), Box::new(NoopProcessorFactory {}));
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

    let runtime = create_test_runtime();
    let runtime_clone = runtime.clone();

    let (sender, receiver) = oneshot::channel::<()>();
    let join_handle = runtime.block_on(async move {
        let (_temp_dir, checkpoint) = create_checkpoint_for_test().await;
        DagExecutor::new(dag, checkpoint, Default::default())
            .await
            .unwrap()
            .start(receiver, Default::default(), runtime_clone)
            .await
            .unwrap()
    });

    thread::sleep(Duration::from_millis(1000));
    sender.send(()).unwrap();
    join_handle.join().unwrap();
}

#[derive(Debug)]
pub(crate) struct NoopJoinProcessorFactory {}

pub const NOOP_JOIN_LEFT_INPUT_PORT: u16 = 1;
pub const NOOP_JOIN_RIGHT_INPUT_PORT: u16 = 2;

#[async_trait]
impl ProcessorFactory for NoopJoinProcessorFactory {
    fn type_name(&self) -> String {
        "NoopJoin".to_owned()
    }

    async fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        Ok(input_schemas.get(&1).unwrap().clone())
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![1, 2]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _checkpoint_data: Option<Vec<u8>>,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        Ok(Box::new(NoopJoinProcessor {}))
    }

    fn id(&self) -> String {
        "NoopJoin".to_owned()
    }
}

#[derive(Debug)]
pub(crate) struct NoopJoinProcessor {}

impl Processor for NoopJoinProcessor {
    fn commit(&self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: OperationWithId,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        fw.send(op, DEFAULT_PORT_HANDLE);
        Ok(())
    }

    fn serialize(&mut self, _object: Object) -> Result<(), BoxedError> {
        Ok(())
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
        Box::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_source(
        source2_handle.clone(),
        Box::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_processor(proc_handle.clone(), Box::new(NoopJoinProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Box::new(CountingSinkFactory::new(count * 2, latch)),
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

    run_dag(dag).unwrap();
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
        Box::new(GeneratorSourceFactory::new(count, latch.clone(), true)),
    );
    dag.add_source(
        source2_handle.clone(),
        Box::new(GeneratorSourceFactory::new(count, latch.clone(), true)),
    );
    dag.add_processor(proc_handle.clone(), Box::new(NoopJoinProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Box::new(CountingSinkFactory::new(count * 2, latch)),
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

    run_dag(dag).unwrap();
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
        Box::new(DualPortGeneratorSourceFactory::new(
            count,
            latch.clone(),
            false,
        )),
    );
    dag.add_processor(proc_handle.clone(), Box::new(NoopJoinProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Box::new(CountingSinkFactory::new(count * 2, latch)),
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

    run_dag(dag).unwrap();
}
