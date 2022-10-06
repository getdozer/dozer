

use dozer_core::aggregation::groupby::{FieldRule, AggregationProcessorFactory};
use tempdir::TempDir;

use dozer_core::dag::dag::{Dag, Endpoint, NodeType};
use dozer_core::dag::mt_executor::{DefaultPortHandle, MultiThreadedDagExecutor};
use dozer_core::state::lmdb::LmdbStateStoreManager;


use dozer_sql::sqlparser::ast::Statement;
use dozer_sql::sqlparser::dialect::GenericDialect;
use dozer_sql::sqlparser::parser::Parser;
use dozer_types::types::Schema;
use dozer_core::aggregation::sum::IntegerSumAggregator;
use crate::get_schema;
use crate::models::connection::Connection;
use crate::pipeline::{CacheSinkFactory, ConnectorSourceFactory};

use super::SimpleOrchestrator;

pub struct Executor {}

impl Executor {
    pub fn run(orchestrator: &SimpleOrchestrator) -> anyhow::Result<()> {
        let mut source_schemas: Vec<Schema> = vec![];
        let mut connections: Vec<Connection> = vec![];
        let mut table_names: Vec<String> = vec![];
        // Get Source schemas
        for source in orchestrator.sources.iter() {
            let schema_tuples = get_schema(source.connection.to_owned())?;

            println!("{:?}", source.table_name);
            let st = schema_tuples
                .iter()
                .find(|t| t.0.eq(&source.table_name))
                .unwrap();

            source_schemas.push(st.to_owned().1);
            connections.push(source.connection.to_owned());
            println!("{:?}", table_names);
            table_names.push(source.table_name.clone());
        }

        let api_endpoint = orchestrator.api_endpoint.as_ref().unwrap();

        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

        let ast = Parser::parse_sql(&dialect, &api_endpoint.sql).unwrap();
        println!("AST: {:?}", ast);
        println!("Schemas: {:?}", source_schemas);
        println!("Query: {:?}", &api_endpoint.sql);
        let _statement: &Statement = &ast[0];

        // let builder = PipelineBuilder::new(source_schemas[0].clone());
        //
        // let (mut dag, in_handle, out_handle) =
        //     builder.statement_to_pipeline(statement.clone()).unwrap();

        let source = ConnectorSourceFactory::new(connections, table_names);

        // let sink = CacheSinkFactory::new(vec![out_handle.port]);
        let sink = CacheSinkFactory::new(vec![DefaultPortHandle]);

        let _source_table_map = source.table_map.clone();

        let mut dag = Dag::new();
        dag.add_node(NodeType::Source(Box::new(source)), 1.to_string());
        dag.add_node(NodeType::Sink(Box::new(sink)), 4.to_string());

        let rules = vec![
            FieldRule::Dimension(1, true, Some("customer_id".to_string())),
            FieldRule::Measure(
                0,
                Box::new(IntegerSumAggregator::new()),
                true,
                Some("Sum".to_string()),
            ),
        ];
        let agg = AggregationProcessorFactory::new(rules);
        dag.add_node(NodeType::Processor(Box::new(agg)), 3.to_string());
        // for (_table_name, endpoint) in in_handle.into_iter() {
        //     // TODO: Use real table_name
        //     let table_name = &"actor".to_string();
        //     let port = source_table_map.get(table_name).unwrap();
        //     dag.connect(Endpoint::new(1.to_string(), port.to_owned()), endpoint)?;
        // }
        //
        // dag.connect(out_handle, Endpoint::new(4.to_string(), DefaultPortHandle))?;

        dag.connect(
            Endpoint::new("1".to_string(), 1),
            Endpoint::new(3.to_string(), DefaultPortHandle),
        )?;

        dag.connect(
            Endpoint::new(3.to_string(), DefaultPortHandle),
            Endpoint::new(4.to_string(), DefaultPortHandle),
        )?;

        let exec = MultiThreadedDagExecutor::new(100000);
        let path = TempDir::new("state-store").unwrap();

        let path_str = path.path().to_str().unwrap().to_string();
        let sm = LmdbStateStoreManager::new(path_str, 1024 * 1024 * 1024 * 5, 20_000).unwrap();

        use std::time::Instant;
        let now = Instant::now();
        exec.start(dag, sm)?;
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);
        Ok(())
    }
}
