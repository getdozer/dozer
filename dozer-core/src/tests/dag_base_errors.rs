use crate::channels::ProcessorChannelForwarder;
use crate::epoch::Epoch;
use crate::node::{
    OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory, Sink, SinkFactory,
    Source, SourceFactory,
};
use crate::tests::dag_base_run::NoopProcessorFactory;
use crate::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::tests::sources::{GeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT};
use crate::{Dag, Endpoint, DEFAULT_PORT_HANDLE};
use dozer_log::storage::{Object, Queue};
use dozer_log::tokio::sync::mpsc::Sender;
use dozer_types::errors::internal::BoxedError;
use dozer_types::models::ingestion_types::IngestionMessage;
use dozer_types::node::{NodeHandle, OpIdentifier};
use dozer_types::tonic::async_trait;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, OperationWithId, Record, Schema, SourceDefinition,
};

use std::collections::HashMap;
use std::panic;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use super::run_dag;

// Test when error is generated by a processor

#[derive(Debug)]
struct ErrorProcessorFactory {
    err_on: u64,
    panic: bool,
}

#[async_trait]
impl ProcessorFactory for ErrorProcessorFactory {
    fn type_name(&self) -> String {
        "Error".to_owned()
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
        Ok(Box::new(ErrorProcessor {
            err_on: self.err_on,
            count: 0,
            panic: self.panic,
        }))
    }

    fn id(&self) -> String {
        "Error".to_owned()
    }
}

#[derive(Debug)]
struct ErrorProcessor {
    err_on: u64,
    count: u64,
    panic: bool,
}

impl Processor for ErrorProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: OperationWithId,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        self.count += 1;
        if self.count == self.err_on {
            if self.panic {
                panic!("Generated error");
            } else {
                return Err("Uknown".to_string().into());
            }
        }

        fw.send(op, DEFAULT_PORT_HANDLE);
        Ok(())
    }

    fn serialize(&mut self, _object: Object) -> Result<(), BoxedError> {
        Ok(())
    }
}

#[test]
#[should_panic]
fn test_run_dag_proc_err_panic() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(None, 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 1.to_string());
    let sink_handle = NodeHandle::new(Some(1), 2.to_string());

    dag.add_source(
        source_handle.clone(),
        Box::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_processor(
        proc_handle.clone(),
        Box::new(ErrorProcessorFactory {
            err_on: 800_000,
            panic: true,
        }),
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

    run_dag(dag).unwrap();
}

#[test]
#[should_panic]
fn test_run_dag_proc_err_2() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(None, 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 1.to_string());
    let proc_err_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Box::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_processor(proc_handle.clone(), Box::new(NoopProcessorFactory {}));

    dag.add_processor(
        proc_err_handle.clone(),
        Box::new(ErrorProcessorFactory {
            err_on: 800_000,
            panic: false,
        }),
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
        Endpoint::new(proc_err_handle.clone(), DEFAULT_PORT_HANDLE),
    )
    .unwrap();

    dag.connect(
        Endpoint::new(proc_err_handle, DEFAULT_PORT_HANDLE),
        Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
    )
    .unwrap();

    run_dag(dag).unwrap();
}

#[test]
#[should_panic]
fn test_run_dag_proc_err_3() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(None, 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 1.to_string());
    let proc_err_handle = NodeHandle::new(Some(1), 2.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Box::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );

    dag.add_processor(
        proc_err_handle.clone(),
        Box::new(ErrorProcessorFactory {
            err_on: 800_000,
            panic: false,
        }),
    );

    dag.add_processor(proc_handle.clone(), Box::new(NoopProcessorFactory {}));

    dag.add_sink(
        sink_handle.clone(),
        Box::new(CountingSinkFactory::new(count, latch)),
    );

    dag.connect(
        Endpoint::new(source_handle, GENERATOR_SOURCE_OUTPUT_PORT),
        Endpoint::new(proc_err_handle.clone(), DEFAULT_PORT_HANDLE),
    )
    .unwrap();

    dag.connect(
        Endpoint::new(proc_err_handle, DEFAULT_PORT_HANDLE),
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

// Test when error is generated by a source

#[derive(Debug)]
pub(crate) struct ErrGeneratorSourceFactory {
    count: u64,
    err_at: u64,
}

impl ErrGeneratorSourceFactory {
    pub fn new(count: u64, err_at: u64) -> Self {
        Self { count, err_at }
    }
}

impl SourceFactory for ErrGeneratorSourceFactory {
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
        "error".to_string()
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            GENERATOR_SOURCE_OUTPUT_PORT,
            OutputPortType::Stateless,
        )]
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
        _state: Option<Vec<u8>>,
    ) -> Result<Box<dyn Source>, BoxedError> {
        Ok(Box::new(ErrGeneratorSource {
            count: self.count,
            err_at: self.err_at,
        }))
    }
}

#[derive(Debug)]
pub(crate) struct ErrGeneratorSource {
    count: u64,
    err_at: u64,
}

#[async_trait]
impl Source for ErrGeneratorSource {
    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError> {
        Ok(vec![])
    }

    async fn start(
        &mut self,
        sender: Sender<(PortHandle, IngestionMessage)>,
        _last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        for n in 1..(self.count + 1) {
            if n == self.err_at {
                return Err("Generated Error".to_string().into());
            }

            sender
                .send((
                    GENERATOR_SOURCE_OUTPUT_PORT,
                    IngestionMessage::OperationEvent {
                        table_index: 0,
                        op: Operation::Insert {
                            new: Record::new(vec![
                                Field::String(format!("key_{n}")),
                                Field::String(format!("value_{n}")),
                            ]),
                        },
                        id: Some(OpIdentifier::new(0, n)),
                    },
                ))
                .await?;
        }
        Ok(())
    }
}

#[test]
#[should_panic]
fn test_run_dag_src_err() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(None, 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 1.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Box::new(ErrGeneratorSourceFactory::new(count, 200_000)),
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

#[derive(Debug)]
pub(crate) struct ErrSinkFactory {
    err_at: u64,
    panic: bool,
}

impl ErrSinkFactory {
    pub fn new(err_at: u64, panic: bool) -> Self {
        Self { err_at, panic }
    }
}

#[async_trait]
impl SinkFactory for ErrSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![COUNTING_SINK_INPUT_PORT]
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        Ok(())
    }

    async fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        Ok(Box::new(ErrSink {
            err_at: self.err_at,
            current: 0,
            panic: self.panic,
        }))
    }

    fn type_name(&self) -> String {
        "error".to_string()
    }
}

#[derive(Debug)]
pub(crate) struct ErrSink {
    err_at: u64,
    current: u64,
    panic: bool,
}
impl Sink for ErrSink {
    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(&mut self, _from_port: PortHandle, _op: OperationWithId) -> Result<(), BoxedError> {
        self.current += 1;
        if self.current == self.err_at {
            if self.panic {
                panic!("Generated error");
            } else {
                return Err("Generated error".to_string().into());
            }
        }
        Ok(())
    }

    fn persist(&mut self, _epoch: &Epoch, _queue: &Queue) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_started(
        &mut self,
        _connection_name: String,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_done(
        &mut self,
        _connection_name: String,
        _id: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn set_source_state(&mut self, _source_state: &[u8]) -> Result<(), BoxedError> {
        Ok(())
    }

    fn get_source_state(&mut self) -> Result<Option<Vec<u8>>, BoxedError> {
        Ok(None)
    }

    fn get_latest_op_id(&mut self) -> Result<Option<OpIdentifier>, BoxedError> {
        Ok(None)
    }
}

#[test]
#[should_panic]
fn test_run_dag_sink_err() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(None, 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 1.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Box::new(GeneratorSourceFactory::new(count, latch, false)),
    );
    dag.add_processor(proc_handle.clone(), Box::new(NoopProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Box::new(ErrSinkFactory::new(200_000, false)),
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
#[should_panic]
fn test_run_dag_sink_err_panic() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(None, 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 1.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Box::new(GeneratorSourceFactory::new(count, latch, false)),
    );
    dag.add_processor(proc_handle.clone(), Box::new(NoopProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Box::new(ErrSinkFactory::new(200_000, true)),
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
