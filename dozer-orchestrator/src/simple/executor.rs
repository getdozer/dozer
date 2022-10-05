use dozer_sql::sqlparser::ast::Statement;
use dozer_sql::sqlparser::dialect::GenericDialect;
use dozer_sql::sqlparser::parser::Parser;

use super::super::pipeline::{sinks::CacheSink, sources::OSource};
use dozer_core::dag::dag::{Endpoint, NodeType};
use dozer_core::dag::mt_executor::{DefaultPortHandle, MultiThreadedDagExecutor};
use dozer_core::state::lmdb::LmdbStateStoreManager;
use dozer_sql::pipeline::builder::PipelineBuilder;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, OperationEvent, Record, Schema,
};

pub struct Executor {}

impl Executor {
    pub fn run() {
        let sql = "SELECT Country, COUNT(Spending), ROUND(SUM(ROUND(Spending))) \
                            FROM Customers \
                            WHERE Spending >= 1000 \
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
        let (mut dag, in_handle, out_handle) =
            builder.statement_to_pipeline(statement.clone()).unwrap();

        let source = TestSourceFactory::new(1, vec![DefaultPortHandle]);
        let sink = TestSinkFactory::new(1, vec![DefaultPortHandle]);

        dag.add_node(NodeType::Source(Box::new(source)), 1);
        dag.add_node(NodeType::Sink(Box::new(sink)), 4);

        let source_to_projection = dag.connect(
            Endpoint::new(1, DefaultPortHandle),
            Endpoint::new(2, DefaultPortHandle),
        );

        let selection_to_sink = dag.connect(
            Endpoint::new(3, DefaultPortHandle),
            Endpoint::new(4, DefaultPortHandle),
        );

        let exec = MultiThreadedDagExecutor::new(100000);
        let sm =
            LmdbStateStoreManager::new("data".to_string(), 1024 * 1024 * 1024 * 5, 20_000).unwrap();

        use std::time::Instant;
        let now = Instant::now();
        exec.start(dag, sm);
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);
    }
}
