use dozer_sql::sqlparser::ast::Statement;
use dozer_sql::sqlparser::dialect::GenericDialect;
use dozer_sql::sqlparser::parser::Parser;

use crate::get_schema;
use crate::models::api_endpoint;

use super::super::pipeline::{CacheSink, IngestionSource};
use super::SimpleOrchestrator;
use dozer_core::dag::dag::{Endpoint, NodeType};
use dozer_core::dag::mt_executor::{DefaultPortHandle, MultiThreadedDagExecutor};
use dozer_core::state::lmdb::LmdbStateStoreManager;
use dozer_sql::pipeline::builder::PipelineBuilder;
use dozer_types::types::{FieldDefinition, FieldType, Schema};

pub struct Executor {}

impl Executor {
    pub fn run(orchestrator: &SimpleOrchestrator) -> anyhow::Result<()> {
        let source_schemas: Vec<Schema> = vec![];
        // Get Source schemas
        for source in orchestrator.sources.iter() {
            let schema_tuples = get_schema(source.connection.to_owned())?;
            let st = schema_tuples
                .iter()
                .find(|t| t.0 == source.table_name)
                .unwrap();

            source_schemas.push(st.to_owned().1);
        }

        let api_endpoint = orchestrator.api_endpoint.unwrap();

        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

        let ast = Parser::parse_sql(&dialect, &api_endpoint.sql).unwrap();
        println!("AST: {:?}", ast);
        let statement: &Statement = &ast[0];

        let builder = PipelineBuilder::new(source_schemas[0]);

        let (mut dag, in_handle, out_handle) =
            builder.statement_to_pipeline(statement.clone()).unwrap();

        let ingestion_source = IngestionSource::new();
        let source = TestSourceFactory::new(1, vec![DefaultPortHandle]);
        let sink = TestSinkFactory::new(1, vec![DefaultPortHandle]);

        dag.add_node(NodeType::Source(Box::new(source)), 1);
        dag.add_node(NodeType::Sink(Box::new(sink)), 4);

        let source_to_projection = dag.connect(
            Endpoint::new(1, DefaultPortHandle),
            Endpoint::new(2, DefaultPortHandle),
        )?;

        let selection_to_sink = dag.connect(
            Endpoint::new(3, DefaultPortHandle),
            Endpoint::new(4, DefaultPortHandle),
        )?;

        let exec = MultiThreadedDagExecutor::new(100000);
        let sm =
            LmdbStateStoreManager::new("data".to_string(), 1024 * 1024 * 1024 * 5, 20_000).unwrap();

        use std::time::Instant;
        let now = Instant::now();
        exec.start(dag, sm);
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);
        Ok(())
    }
}
