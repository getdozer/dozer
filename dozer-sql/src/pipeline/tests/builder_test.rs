use std::collections::HashMap;

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use dozer_core::dag::dag::{Endpoint, NodeType, PortHandle};
use dozer_core::dag::forwarder::{ChannelManager, SourceChannelForwarder};
use dozer_core::dag::mt_executor::{DefaultPortHandle, MultiThreadedDagExecutor};
use dozer_core::dag::node::NextStep::Continue;
use dozer_core::dag::node::{NextStep, Sink, SinkFactory, Source, SourceFactory};
use dozer_core::state::lmdb::LmdbStateStoreManager;
use dozer_core::state::StateStore;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, OperationEvent, Record, Schema,
};

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
    ) -> anyhow::Result<()> {
        for n in 0..100 {
            fw.send(
                OperationEvent::new(
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
                ),
                DefaultPortHandle,
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
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn init(&mut self, _state_store: &mut dyn StateStore) -> anyhow::Result<()> {
        println!("SINK: Initialising TestSink");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _op: OperationEvent,
        _state: &mut dyn StateStore,
    ) -> anyhow::Result<NextStep> {
        //    println!("SINK: Message {} received", _op.seq_no);
        Ok(Continue)
    }
}

#[test]
fn test_pipeline_builder() {
    let sql = "SELECT 1+1.0, Country, COUNT(Spending+2000), ROUND(SUM(ROUND(-Spending))) \
                            FROM Customers \
                            WHERE Spending+500 >= 1000 \
                            GROUP BY Country \
                            HAVING COUNT(CustomerID) > 1;";

    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    println!("AST: {:?}", ast);

    let statement: &Statement = &ast[0];

    let schema = Schema {
        fields: vec![
            FieldDefinition {
                name: String::from("CustomerID"),
                typ: FieldType::Int,
                nullable: false,
            },
            FieldDefinition {
                name: String::from("Country"),
                typ: FieldType::String,
                nullable: false,
            },
            FieldDefinition {
                name: String::from("Spending"),
                typ: FieldType::Int,
                nullable: false,
            },
        ],
        values: vec![0],
        primary_index: vec![],
        secondary_indexes: vec![],
        identifier: None,
    };

    let builder = PipelineBuilder::new(schema);
    let (mut dag, mut in_handle, out_handle) =
        builder.statement_to_pipeline(statement.clone()).unwrap();

    let source = TestSourceFactory::new(vec![DefaultPortHandle]);
    let sink = TestSinkFactory::new(vec![DefaultPortHandle]);

    dag.add_node(NodeType::Source(Box::new(source)), 1.to_string());
    dag.add_node(NodeType::Sink(Box::new(sink)), 4.to_string());

    let input_point = in_handle.remove("customers").unwrap();

    let _source_to_projection = dag.connect(
        Endpoint::new(1.to_string(), DefaultPortHandle),
        Endpoint::new(input_point.node, input_point.port),
    );

    let _selection_to_sink = dag.connect(
        Endpoint::new(out_handle.node, out_handle.port),
        Endpoint::new(4.to_string(), DefaultPortHandle),
    );

    let exec = MultiThreadedDagExecutor::new(100000);
    let sm =
        LmdbStateStoreManager::new("data".to_string(), 1024 * 1024 * 1024 * 5, 20_000).unwrap();

    use std::time::Instant;
    let now = Instant::now();
    let _ = exec.start(dag, sm);
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
}
