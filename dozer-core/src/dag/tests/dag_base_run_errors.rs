use crate::chk;
use crate::dag::channels::{ProcessorChannelForwarder, SourceChannelForwarder};
use crate::dag::dag::{Dag, Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use crate::dag::errors::ExecutionError;
use crate::dag::executor::{DagExecutor, ExecutorOptions};
use crate::dag::node::{
    OutputPortDef, OutputPortDefOptions, PortHandle, Processor, ProcessorFactory, Sink,
    SinkFactory, Source, SourceFactory,
};
use crate::dag::record_store::RecordReader;
use crate::dag::tests::common::init_log4rs;
use crate::dag::tests::dag_base_run::NoopProcessorFactory;
use crate::dag::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::dag::tests::sources::{
    GeneratorSource, GeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT,
};
use crate::storage::common::{Environment, RwTransaction};
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use fp_rust::sync::CountDownLatch;
use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempdir::TempDir;

// Test when error is generated by a processor

struct ErrorProcessorFactory {
    err_on: u64,
}

impl ProcessorFactory for ErrorProcessorFactory {
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
        Box::new(ErrorProcessor {
            err_on: self.err_on,
            count: 0,
        })
    }
}

struct ErrorProcessor {
    err_on: u64,
    count: u64,
}

impl Processor for ErrorProcessor {
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
        self.count += 1;
        if self.count == self.err_on {
            return Err(ExecutionError::InvalidOperation("Uknown".to_string()));
        }

        fw.send(op, DEFAULT_PORT_HANDLE)
    }
}

#[test]
fn test_run_dag_proc_err() {
    init_log4rs();

    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(CountDownLatch::new(0));

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(count, latch.clone()))),
        "source".to_string(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(ErrorProcessorFactory { err_on: 800_000 })),
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
    assert!(executor.join().is_err());
}

#[test]
fn test_run_dag_proc_err_2() {
    init_log4rs();

    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(CountDownLatch::new(1));

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(count, latch.clone()))),
        "source".to_string(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopProcessorFactory {})),
        "proc".to_string(),
    );

    dag.add_node(
        NodeType::Processor(Arc::new(ErrorProcessorFactory { err_on: 800_000 })),
        "proc_err".to_string(),
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
        Endpoint::new("proc_err".to_string(), DEFAULT_PORT_HANDLE),
    ));

    chk!(dag.connect(
        Endpoint::new("proc_err".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("sink".to_string(), COUNTING_SINK_INPUT_PORT),
    ));

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        &tmp_dir.path(),
        ExecutorOptions::default()
    ));

    chk!(executor.start());
    assert!(executor.join().is_err());
}

// Test when error is generated by a source

pub(crate) struct ErrGeneratorSourceFactory {
    count: u64,
    term_latch: Option<Arc<CountDownLatch>>,
    err_at: u64,
}

impl ErrGeneratorSourceFactory {
    pub fn new(count: u64, err_at: u64, term_latch: Option<Arc<CountDownLatch>>) -> Self {
        Self {
            count,
            term_latch,
            err_at,
        }
    }
}

impl SourceFactory for ErrGeneratorSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, ExecutionError> {
        Ok(Schema::empty()
            .field(
                FieldDefinition::new("id".to_string(), FieldType::String, false),
                true,
                true,
            )
            .field(
                FieldDefinition::new("value".to_string(), FieldType::String, false),
                true,
                false,
            )
            .clone())
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            GENERATOR_SOURCE_OUTPUT_PORT,
            OutputPortDefOptions::default(),
        )]
    }
    fn build(&self) -> Box<dyn Source> {
        Box::new(ErrGeneratorSource {
            count: self.count,
            err_at: self.err_at,
            term_latch: if let Some(latch) = self.term_latch.as_ref() {
                Some(latch.clone())
            } else {
                None
            },
        })
    }
}

pub(crate) struct ErrGeneratorSource {
    count: u64,
    term_latch: Option<Arc<CountDownLatch>>,
    err_at: u64,
}

impl Source for ErrGeneratorSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<u64>,
    ) -> Result<(), ExecutionError> {
        for n in 1..(self.count + 1) {
            if n == self.err_at {
                return Err(ExecutionError::InvalidOperation(
                    "Generated Error".to_string(),
                ));
            }

            fw.send(
                n,
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::String(format!("key_{}", n)),
                            Field::String(format!("value_{}", n)),
                        ],
                    ),
                },
                GENERATOR_SOURCE_OUTPUT_PORT,
            )?;
        }
        if let Some(latch) = &self.term_latch {
            latch.wait();
        }
        Ok(())
    }
}

#[test]
fn test_run_dag_src_err() {
    init_log4rs();

    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(CountDownLatch::new(1));

    dag.add_node(
        NodeType::Source(Arc::new(ErrGeneratorSourceFactory::new(
            count,
            200_000,
            Some(latch.clone()),
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

pub(crate) struct ErrSinkFactory {
    err_at: u64,
    latch: Arc<CountDownLatch>,
}

impl ErrSinkFactory {
    pub fn new(err_at: u64, latch: Arc<CountDownLatch>) -> Self {
        Self { err_at, latch }
    }
}

impl SinkFactory for ErrSinkFactory {
    fn set_input_schema(
        &self,
        output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![COUNTING_SINK_INPUT_PORT]
    }
    fn build(&self) -> Box<dyn Sink> {
        Box::new(ErrSink {
            err_at: self.err_at,
            current: 0,
            latch: self.latch.clone(),
        })
    }
}

pub(crate) struct ErrSink {
    err_at: u64,
    current: u64,
    latch: Arc<CountDownLatch>,
}
impl Sink for ErrSink {
    fn init(&mut self, _state: &mut dyn Environment) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn commit(&self, _tx: &mut dyn RwTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _seq: u64,
        _op: Operation,
        _state: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        self.current += 1;
        if self.current == self.err_at {
            self.latch.countdown();
            return Err(ExecutionError::InvalidOperation(
                "Generated error".to_string(),
            ));
        }
        self.latch.countdown();
        Ok(())
    }
}

#[test]
fn test_run_dag_sink_err() {
    init_log4rs();

    let count: u64 = 1_000_000;

    let mut dag = Dag::new();
    let latch = Arc::new(CountDownLatch::new(1));

    dag.add_node(
        NodeType::Source(Arc::new(GeneratorSourceFactory::new(count, latch.clone()))),
        "source".to_string(),
    );
    dag.add_node(
        NodeType::Processor(Arc::new(NoopProcessorFactory {})),
        "proc".to_string(),
    );
    dag.add_node(
        NodeType::Sink(Arc::new(ErrSinkFactory::new(200_000, latch.clone()))),
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
    assert!(executor.join().is_err());
}
