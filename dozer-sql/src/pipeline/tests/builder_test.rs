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
use dozer_core::dag::tests::sinks::CountingSinkFactory;
use dozer_core::dag::tests::sources::GeneratorSourceFactory;
use dozer_core::storage::common::{Environment, RwTransaction};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use log::debug;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, Barrier};
use std::time::Duration;
use tempdir::TempDir;

fn get_schema() -> Schema {
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
        .clone()
}

pub fn get_op_gen() -> &'static (dyn Fn(u64) -> Operation + Send + Sync) {
    &|n: u64| Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(5.5)),
            ],
        ),
    }
}

#[test]
fn test_pipeline_builder() {
    let sql = "SELECT Country, SUM(Spending) \
                            FROM Customers \
                            WHERE Spending >= 1 GROUP BY Country";

    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    debug!("AST: {:?}", ast);

    let statement: &Statement = &ast[0];

    let builder = PipelineBuilder {};
    let (mut dag, mut in_handle, out_handle) =
        builder.statement_to_pipeline(statement.clone()).unwrap();

    let sync = Arc::new(Barrier::new(2));

    let source = GeneratorSourceFactory::new(
        10_000,
        Duration::from_millis(0),
        sync.clone(),
        false,
        get_schema(),
        get_op_gen(),
    );

    let sink = CountingSinkFactory::new(10_000, sync.clone());

    dag.add_node(NodeType::Source(Box::new(source)), "source".to_string());
    dag.add_node(NodeType::Sink(Box::new(sink)), "sink".to_string());

    let input_point = in_handle.remove("customers").unwrap();

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
