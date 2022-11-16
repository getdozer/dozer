#![allow(non_snake_case)]
use crate::dag::channels::ProcessorChannelForwarder;
use crate::dag::dag::{Dag, Endpoint, NodeType};
use crate::dag::errors::ExecutionError;
use crate::dag::executor_local::{ExecutorOptions, MultiThreadedDagExecutor};
use crate::dag::node::{
    NodeHandle, PortHandle, StatefulPortHandle, StatefulProcessor, StatefulProcessorFactory,
    StatelessProcessor, StatelessProcessorFactory,
};
use crate::dag::record_store::RecordReader;
use crate::dag::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::dag::tests::sources::{
    GeneratorSourceFactory, StatefulGeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT,
};
use crate::storage::common::{Environment, RwTransaction};
use dozer_types::types::{Field, Operation, Schema};
use std::collections::HashMap;
use std::time::Duration;
use std::{fs, thread};
use tempdir::TempDir;

macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

pub(crate) const PASSTHROUGH_PROCESSOR_INPUT_PORT: PortHandle = 50;
pub(crate) const PASSTHROUGH_PROCESSOR_OUTPUT_PORT: PortHandle = 60;

pub(crate) struct PassthroughProcessorFactory {}

impl PassthroughProcessorFactory {
    pub fn new() -> Self {
        Self {}
    }
}

impl StatefulProcessorFactory for PassthroughProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![PASSTHROUGH_PROCESSOR_INPUT_PORT]
    }
    fn get_output_ports(&self) -> Vec<StatefulPortHandle> {
        vec![StatefulPortHandle::new(
            PASSTHROUGH_PROCESSOR_OUTPUT_PORT,
            true,
        )]
    }
    fn build(&self) -> Box<dyn StatefulProcessor> {
        Box::new(PassthroughProcessor {})
    }
}

pub(crate) struct PassthroughProcessor {}

impl StatefulProcessor for PassthroughProcessor {
    fn update_schema(
        &mut self,
        _output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        Ok(input_schemas
            .get(&PASSTHROUGH_PROCESSOR_INPUT_PORT)
            .unwrap()
            .clone())
    }

    fn init<'a>(&'_ mut self, _tx: &mut dyn Environment) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        _tx: &mut dyn RwTransaction,
        _readers: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        fw.send(op, PASSTHROUGH_PROCESSOR_OUTPUT_PORT)
    }
}

pub(crate) struct RecordReaderProcessorFactory {}

impl RecordReaderProcessorFactory {
    pub fn new() -> Self {
        Self {}
    }
}

pub(crate) const RECORD_READER_PROCESSOR_INPUT_PORT: PortHandle = 70;
pub(crate) const RECORD_READER_PROCESSOR_OUTPUT_PORT: PortHandle = 80;

impl StatelessProcessorFactory for RecordReaderProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![RECORD_READER_PROCESSOR_INPUT_PORT]
    }
    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![RECORD_READER_PROCESSOR_OUTPUT_PORT]
    }
    fn build(&self) -> Box<dyn StatelessProcessor> {
        Box::new(RecordReaderProcessor { ctr: 0 })
    }
}

pub(crate) struct RecordReaderProcessor {
    ctr: u64,
}

impl StatelessProcessor for RecordReaderProcessor {
    fn update_schema(
        &mut self,
        _output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        Ok(input_schemas
            .get(&RECORD_READER_PROCESSOR_INPUT_PORT)
            .unwrap()
            .clone())
    }

    fn init<'a>(&'_ mut self) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        readers: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        let v = readers
            .get(&RECORD_READER_PROCESSOR_INPUT_PORT)
            .unwrap()
            .get(
                Field::String(format!("key_{}", self.ctr))
                    .to_bytes()?
                    .as_slice(),
            )?;
        assert!(v.is_some());
        self.ctr += 1;

        fw.send(op, RECORD_READER_PROCESSOR_OUTPUT_PORT)
    }
}

#[test]
fn test_run_dag_reacord_reader() {
    log4rs::init_file("../log4rs.sample.yaml", Default::default())
        .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));

    let src = GeneratorSourceFactory::new(1_000_000);
    let passthrough = PassthroughProcessorFactory::new();
    let record_reader = RecordReaderProcessorFactory::new();
    let sink = CountingSinkFactory::new(1_000);

    let mut dag = Dag::new();

    let SOURCE_ID: NodeHandle = "source".to_string();
    let PASSTHROUGH_ID: NodeHandle = "passthrough".to_string();
    let RECORD_READER_ID: NodeHandle = "record_reader".to_string();
    let SINK_ID: NodeHandle = "sink".to_string();

    dag.add_node(NodeType::StatelessSource(Box::new(src)), SOURCE_ID.clone());
    dag.add_node(
        NodeType::StatefulProcessor(Box::new(passthrough)),
        PASSTHROUGH_ID.clone(),
    );
    dag.add_node(
        NodeType::StatelessProcessor(Box::new(record_reader)),
        RECORD_READER_ID.clone(),
    );
    dag.add_node(NodeType::StatefulSink(Box::new(sink)), SINK_ID.clone());

    assert!(dag
        .connect(
            Endpoint::new(SOURCE_ID, GENERATOR_SOURCE_OUTPUT_PORT),
            Endpoint::new(PASSTHROUGH_ID.clone(), PASSTHROUGH_PROCESSOR_INPUT_PORT),
        )
        .is_ok());

    assert!(dag
        .connect(
            Endpoint::new(PASSTHROUGH_ID, PASSTHROUGH_PROCESSOR_OUTPUT_PORT),
            Endpoint::new(RECORD_READER_ID.clone(), RECORD_READER_PROCESSOR_INPUT_PORT),
        )
        .is_ok());

    assert!(dag
        .connect(
            Endpoint::new(RECORD_READER_ID, RECORD_READER_PROCESSOR_OUTPUT_PORT),
            Endpoint::new(SINK_ID, COUNTING_SINK_INPUT_PORT),
        )
        .is_ok());

    let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to remove old dir"));
    }
    fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

    let exec = chk!(MultiThreadedDagExecutor::start(
        dag,
        tmp_dir.into_path(),
        ExecutorOptions::default()
    ));

    thread::sleep(Duration::from_millis(5_000));

    exec.stop();
    exec.join();

    // thread::sleep(Duration::from_millis(20_000));
}

// #[test]
// fn test_run_dag_reacord_reader_from_stateful_src() {
//     // log4rs::init_file("../log4rs.sample.yaml", Default::default())
//     //     .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));
//
//     let src = StatefulGeneratorSourceFactory::new(1_000);
//     let record_reader = RecordReaderProcessorFactory::new();
//     let sink = CountingSinkFactory::new(1_000);
//
//     let mut dag = Dag::new();
//
//     let SOURCE_ID: NodeHandle = "source".to_string();
//     let RECORD_READER_ID: NodeHandle = "record_reader".to_string();
//     let SINK_ID: NodeHandle = "sink".to_string();
//
//     dag.add_node(NodeType::StatefulSource(Box::new(src)), SOURCE_ID.clone());
//     dag.add_node(
//         NodeType::StatelessProcessor(Box::new(record_reader)),
//         RECORD_READER_ID.clone(),
//     );
//     dag.add_node(NodeType::StatefulSink(Box::new(sink)), SINK_ID.clone());
//
//     assert!(dag
//         .connect(
//             Endpoint::new(SOURCE_ID, GENERATOR_SOURCE_OUTPUT_PORT),
//             Endpoint::new(RECORD_READER_ID.clone(), RECORD_READER_PROCESSOR_INPUT_PORT),
//         )
//         .is_ok());
//
//     assert!(dag
//         .connect(
//             Endpoint::new(RECORD_READER_ID, RECORD_READER_PROCESSOR_OUTPUT_PORT),
//             Endpoint::new(SINK_ID, COUNTING_SINK_INPUT_PORT),
//         )
//         .is_ok());
//
//     let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
//     if tmp_dir.path().exists() {
//         fs::remove_dir_all(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to remove old dir"));
//     }
//     fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));
//
//     let exec = MultiThreadedDagExecutor::new(20_000 - 1, 20_000);
//
//     assert!(exec.start(dag, tmp_dir.into_path()).is_ok());
// }
