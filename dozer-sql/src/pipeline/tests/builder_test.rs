use crate::pipeline::builder::PipelineBuilder;
use dozer_core::dag::channels::SourceChannelForwarder;
use dozer_core::dag::dag::{Endpoint, NodeType};
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::executor_local::ExecutorOptions;
use dozer_core::dag::executor_local::{MultiThreadedDagExecutor, DEFAULT_PORT_HANDLE};
use dozer_core::dag::node::{
    OutputPortDef, OutputPortDefOptions, PortHandle, Sink, SinkFactory, Source, SourceFactory,
};
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::{Environment, RwTransaction};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use log::debug;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::fs;
use tempdir::TempDir;

/// Test Source
pub struct TestSourceFactory {
    output_ports: Vec<PortHandle>,
}

impl TestSourceFactory {
    pub fn new(output_ports: Vec<PortHandle>) -> Self {
        Self { output_ports }
    }
}

impl SourceFactory for TestSourceFactory {
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.output_ports
            .iter()
            .map(|e| OutputPortDef::new(*e, OutputPortDefOptions::default()))
            .collect()
    }
    fn build(&self) -> Box<dyn Source> {
        Box::new(TestSource {})
    }
}

pub struct TestSource {}

impl Source for TestSource {
    fn get_output_schema(&self, _port: PortHandle) -> Option<Schema> {
        Some(
            Schema::empty()
                .field(
                    FieldDefinition::new(String::from("CustomerID"), FieldType::Int, false),
                    false,
                    false,
                )
                .field(
                    FieldDefinition::new(String::from("Country"), FieldType::String, false),
                    false,
                    false,
                )
                .field(
                    FieldDefinition::new(String::from("Spending"), FieldType::Float, false),
                    false,
                    false,
                )
                .clone(),
        )
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<u64>,
    ) -> Result<(), ExecutionError> {
        for n in 0..10000 {
            fw.send(
                n,
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(0),
                            Field::String("Italy".to_string()),
                            Field::Float(OrderedFloat(5.5)),
                        ],
                    ),
                },
                DEFAULT_PORT_HANDLE,
            )
            .unwrap();
        }
        fw.terminate().unwrap();
        Ok(())
    }
}

pub struct TestSinkFactory {
    input_ports: Vec<PortHandle>,
}

impl TestSinkFactory {
    pub fn new(input_ports: Vec<PortHandle>) -> Self {
        Self { input_ports }
    }
}

impl SinkFactory for TestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }
    fn build(&self) -> Box<dyn Sink> {
        Box::new(TestSink {})
    }
}

pub struct TestSink {}

impl Sink for TestSink {
    fn update_schema(
        &mut self,
        _input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn init(&mut self, _env: &mut dyn Environment) -> Result<(), ExecutionError> {
        debug!("SINK: Initialising TestSink");
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
        Ok(())
    }
}

#[test]
fn test_pipeline_builder() {
    let sql = "SELECT Country, SUM(Spending) \
                            FROM Users \
                            WHERE Spending >= 1 GROUP BY Country";

    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    debug!("AST: {:?}", ast);

    let statement: &Statement = &ast[0];

    let builder = PipelineBuilder {};
    let (mut dag, mut in_handle, out_handle) =
        builder.statement_to_pipeline(statement.clone()).unwrap();

    let source = TestSourceFactory::new(vec![DEFAULT_PORT_HANDLE]);
    let sink = TestSinkFactory::new(vec![DEFAULT_PORT_HANDLE]);

    dag.add_node(NodeType::Source(Box::new(source)), "source".to_string());
    dag.add_node(NodeType::Sink(Box::new(sink)), "sink".to_string());

    let input_point = in_handle.remove("users").unwrap();

    let _source_to_input = dag.connect(
        Endpoint::new("source".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new(input_point.node, input_point.port),
    );

    let _output_to_sink = dag.connect(
        Endpoint::new(out_handle.node, out_handle.port),
        Endpoint::new("sink".to_string(), DEFAULT_PORT_HANDLE),
    );

    let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to remove old dir"));
    }
    fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

    use std::time::Instant;
    let now = Instant::now();

    let exec =
        MultiThreadedDagExecutor::start(dag, tmp_dir.path(), ExecutorOptions::default()).unwrap();

    exec.join().unwrap();
    let elapsed = now.elapsed();
    debug!("Elapsed: {:.2?}", elapsed);
}
