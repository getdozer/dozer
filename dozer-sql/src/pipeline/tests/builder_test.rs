use dozer_core::dag::dag::{Endpoint, NodeType};
use dozer_core::dag::mt_executor::{MultiThreadedDagExecutor, DEFAULT_PORT_HANDLE};
use dozer_core::state::lmdb::LmdbStateStoreManager;
use dozer_types::core::channels::{ChannelManager, SourceChannelForwarder};
use dozer_types::core::node::{PortHandle, Sink, SinkFactory, Source, SourceFactory};
use dozer_types::core::state::{StateStore, StateStoreOptions};
use dozer_types::errors::execution::ExecutionError;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use log::debug;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tempdir::TempDir;

use crate::pipeline::builder::PipelineBuilder;

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
    fn get_state_store_opts(&self) -> Option<StateStoreOptions> {
        None
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.output_ports.clone()
    }
    fn build(&self) -> Box<dyn Source> {
        Box::new(TestSource {})
    }
}

pub struct TestSource {}

impl Source for TestSource {
    fn get_output_schema(&self, _port: PortHandle) -> Schema {
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
                FieldDefinition::new(String::from("Spending"), FieldType::Int, false),
                false,
                false,
            )
            .clone()
    }

    fn start(
        &self,
        fw: &dyn SourceChannelForwarder,
        cm: &dyn ChannelManager,
        _state: &mut dyn StateStore,
        _from_seq: Option<u64>,
    ) -> Result<(), ExecutionError> {
        for n in 0..10000000 {
            fw.send(
                n,
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(0),
                            Field::String("Italy".to_string()),
                            Field::Int(2000),
                        ],
                    ),
                },
                DEFAULT_PORT_HANDLE,
            )
            .unwrap();
        }
        cm.terminate().unwrap();
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
    fn get_state_store_opts(&self) -> Option<StateStoreOptions> {
        None
    }

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

    fn init(&mut self, _state_store: &mut dyn StateStore) -> Result<(), ExecutionError> {
        debug!("SINK: Initialising TestSink");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _seq: u64,
        _op: Operation,
        _state: &mut dyn StateStore,
    ) -> Result<(), ExecutionError> {
        //    debug!("SINK: Message {} received", _op.seq_no);
        Ok(())
    }
}

#[test]
fn test_pipeline_builder() {
    let sql = "SELECT 1+1.0, Country, COUNT(Spending+2000), ROUND(SUM(ROUND(-Spending))) \
                            FROM Customers \
                            WHERE Spending+500 >= 1000 \
                            GROUP BY ROUND(Country) \
                            HAVING COUNT(CustomerID) > 1;";

    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    debug!("AST: {:?}", ast);

    let statement: &Statement = &ast[0];

    let builder = PipelineBuilder {};
    let (mut dag, mut in_handle, out_handle) =
        builder.statement_to_pipeline(statement.clone()).unwrap();

    let source = TestSourceFactory::new(vec![DEFAULT_PORT_HANDLE]);
    let sink = TestSinkFactory::new(vec![DEFAULT_PORT_HANDLE]);

    dag.add_node(NodeType::Source(Box::new(source)), 1.to_string());
    dag.add_node(NodeType::Sink(Box::new(sink)), 4.to_string());

    let input_point = in_handle.remove("customers").unwrap();

    let _source_to_input = dag.connect(
        Endpoint::new(1.to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new(input_point.node, input_point.port),
    );

    let _output_to_sink = dag.connect(
        Endpoint::new(out_handle.node, out_handle.port),
        Endpoint::new(4.to_string(), DEFAULT_PORT_HANDLE),
    );

    let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to remove old dir"));
    }
    fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

    let exec = MultiThreadedDagExecutor::new(100000);
    let sm = Arc::new(LmdbStateStoreManager::new(
        tmp_dir.path().to_str().unwrap().to_string(),
        1024 * 1024 * 1024 * 5,
        20_000,
    ));

    use std::time::Instant;
    let now = Instant::now();
    let _ = exec.start(dag, sm);
    let elapsed = now.elapsed();
    debug!("Elapsed: {:.2?}", elapsed);
}
