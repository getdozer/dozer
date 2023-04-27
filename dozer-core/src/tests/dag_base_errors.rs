use crate::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::errors::ExecutionError;
use crate::executor::{DagExecutor, ExecutorOptions};
use crate::node::{
    OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory, Sink, SinkFactory,
    Source, SourceFactory,
};
use crate::tests::dag_base_run::NoopProcessorFactory;
use crate::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::tests::sources::{GeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT};
use crate::{Dag, Endpoint, DEFAULT_PORT_HANDLE};
use dozer_types::epoch::Epoch;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::node::NodeHandle;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};

use std::collections::HashMap;
use std::panic;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::tests::app::NoneContext;

// Test when error is generated by a processor

#[derive(Debug)]
struct ErrorProcessorFactory {
    err_on: u64,
    panic: bool,
}

impl ProcessorFactory<NoneContext> for ErrorProcessorFactory {
    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(Schema, NoneContext), ExecutionError> {
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
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        Ok(Box::new(ErrorProcessor {
            err_on: self.err_on,
            count: 0,
            panic: self.panic,
        }))
    }
}

#[derive(Debug)]
struct ErrorProcessor {
    err_on: u64,
    count: u64,
    panic: bool,
}

impl Processor for ErrorProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), ExecutionError> {
        self.count += 1;
        if self.count == self.err_on {
            if self.panic {
                panic!("Generated error");
            } else {
                return Err(ExecutionError::InvalidOperation("Uknown".to_string()));
            }
        }

        fw.send(op, DEFAULT_PORT_HANDLE)
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
        Arc::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
    );
    dag.add_processor(
        proc_handle.clone(),
        Arc::new(ErrorProcessorFactory {
            err_on: 800_000,
            panic: true,
        }),
    );
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

// These tests doesnt pass anymore because processor is not panicing
// TODO: Enable tests when errors threshold is implemented
// #[test]
// #[should_panic]
// fn test_run_dag_proc_err_2() {
//     let count: u64 = 1_000_000;
//
//     let mut dag = Dag::new();
//     let latch = Arc::new(AtomicBool::new(true));
//
//     let source_handle = NodeHandle::new(None, 1.to_string());
//     let proc_handle = NodeHandle::new(Some(1), 1.to_string());
//     let proc_err_handle = NodeHandle::new(Some(1), 2.to_string());
//     let sink_handle = NodeHandle::new(Some(1), 3.to_string());
//
//     dag.add_source(
//         source_handle.clone(),
//         Arc::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
//     );
//     dag.add_processor(proc_handle.clone(), Arc::new(NoopProcessorFactory {}));
//
//     dag.add_processor(
//         proc_err_handle.clone(),
//         Arc::new(ErrorProcessorFactory {
//             err_on: 800_000,
//             panic: false,
//         }),
//     );
//
//     dag.add_sink(
//         sink_handle.clone(),
//         Arc::new(CountingSinkFactory::new(count, latch)),
//     );
//
//     dag.connect(
//         Endpoint::new(source_handle, GENERATOR_SOURCE_OUTPUT_PORT),
//         Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
//     ).unwrap();
//
//     dag.connect(
//         Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
//         Endpoint::new(proc_err_handle.clone(), DEFAULT_PORT_HANDLE),
//     ).unwrap();
//
//     dag.connect(
//         Endpoint::new(proc_err_handle, DEFAULT_PORT_HANDLE),
//         Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
//     ).unwrap();
//
//     DagExecutor::new(
//         dag,
//         ExecutorOptions::default(),
//     )
//     .unwrap()
//     .start(Arc::new(AtomicBool::new(true)))
//     .unwrap()
//     .join()
//     .unwrap();
// }
//
// #[test]
// #[should_panic]
// fn test_run_dag_proc_err_3() {
//     let count: u64 = 1_000_000;
//
//     let mut dag = Dag::new();
//     let latch = Arc::new(AtomicBool::new(true));
//
//     let source_handle = NodeHandle::new(None, 1.to_string());
//     let proc_handle = NodeHandle::new(Some(1), 1.to_string());
//     let proc_err_handle = NodeHandle::new(Some(1), 2.to_string());
//     let sink_handle = NodeHandle::new(Some(1), 3.to_string());
//
//     dag.add_source(
//         source_handle.clone(),
//         Arc::new(GeneratorSourceFactory::new(count, latch.clone(), false)),
//     );
//
//     dag.add_processor(
//         proc_err_handle.clone(),
//         Arc::new(ErrorProcessorFactory {
//             err_on: 800_000,
//             panic: false,
//         }),
//     );
//
//     dag.add_processor(proc_handle.clone(), Arc::new(NoopProcessorFactory {}));
//
//     dag.add_sink(
//         sink_handle.clone(),
//         Arc::new(CountingSinkFactory::new(count, latch)),
//     );
//
//     dag.connect(
//         Endpoint::new(source_handle, GENERATOR_SOURCE_OUTPUT_PORT),
//         Endpoint::new(proc_err_handle.clone(), DEFAULT_PORT_HANDLE),
//     ).unwrap();
//
//     dag.connect(
//         Endpoint::new(proc_err_handle, DEFAULT_PORT_HANDLE),
//         Endpoint::new(proc_handle.clone(), DEFAULT_PORT_HANDLE),
//     ).unwrap();
//
//     dag.connect(
//         Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
//         Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
//     ).unwrap();
//
//     DagExecutor::new(
//         dag,
//         ExecutorOptions::default(),
//     )
//     .unwrap()
//     .start(Arc::new(AtomicBool::new(true)))
//     .unwrap()
//     .join()
//     .unwrap();
// }

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

impl SourceFactory<NoneContext> for ErrGeneratorSourceFactory {
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

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            GENERATOR_SOURCE_OUTPUT_PORT,
            OutputPortType::Stateless,
        )]
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
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

impl Source for ErrGeneratorSource {
    fn can_start_from(&self, _last_checkpoint: (u64, u64)) -> Result<bool, ExecutionError> {
        Ok(false)
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _checkpoint: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        for n in 1..(self.count + 1) {
            if n == self.err_at {
                return Err(ExecutionError::InvalidOperation(
                    "Generated Error".to_string(),
                ));
            }

            fw.send(
                IngestionMessage::new_op(
                    n,
                    0,
                    Operation::Insert {
                        new: Record::new(
                            None,
                            vec![
                                Field::String(format!("key_{n}")),
                                Field::String(format!("value_{n}")),
                            ],
                        ),
                    },
                ),
                GENERATOR_SOURCE_OUTPUT_PORT,
            )?;
        }
        Ok(())
    }
}

#[test]
fn test_run_dag_src_err() {
    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(AtomicBool::new(true));

    let source_handle = NodeHandle::new(None, 1.to_string());
    let proc_handle = NodeHandle::new(Some(1), 1.to_string());
    let sink_handle = NodeHandle::new(Some(1), 3.to_string());

    dag.add_source(
        source_handle.clone(),
        Arc::new(ErrGeneratorSourceFactory::new(count, 200_000)),
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

    let _join_handle = DagExecutor::new(dag, ExecutorOptions::default())
        .unwrap()
        .start(Arc::new(AtomicBool::new(true)))
        .unwrap();
    // join_handle.join().unwrap();
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

impl SinkFactory<NoneContext> for ErrSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![COUNTING_SINK_INPUT_PORT]
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, ExecutionError> {
        Ok(Box::new(ErrSink {
            err_at: self.err_at,
            current: 0,
            panic: self.panic,
        }))
    }
}

#[derive(Debug)]
pub(crate) struct ErrSink {
    err_at: u64,
    current: u64,
    panic: bool,
}
impl Sink for ErrSink {
    fn commit(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(&mut self, _from_port: PortHandle, _op: Operation) -> Result<(), ExecutionError> {
        self.current += 1;
        if self.current == self.err_at {
            if self.panic {
                panic!("Generated error");
            } else {
                return Err(ExecutionError::InvalidOperation(
                    "Generated error".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self) -> Result<(), ExecutionError> {
        Ok(())
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
        Arc::new(GeneratorSourceFactory::new(count, latch, false)),
    );
    dag.add_processor(proc_handle.clone(), Arc::new(NoopProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Arc::new(ErrSinkFactory::new(200_000, false)),
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
        Arc::new(GeneratorSourceFactory::new(count, latch, false)),
    );
    dag.add_processor(proc_handle.clone(), Arc::new(NoopProcessorFactory {}));
    dag.add_sink(
        sink_handle.clone(),
        Arc::new(ErrSinkFactory::new(200_000, true)),
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
