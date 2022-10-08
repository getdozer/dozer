use std::sync::Arc;

use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::source::Source;
use tempdir::TempDir;

use dozer_cache::cache::lmdb::cache::LmdbCache;
use dozer_core::dag::dag::{Endpoint, NodeType};
use dozer_core::dag::mt_executor::{DefaultPortHandle, MultiThreadedDagExecutor};
use dozer_core::state::lmdb::LmdbStateStoreManager;
use dozer_sql::pipeline::builder::PipelineBuilder;
use dozer_sql::sqlparser::ast::Statement;
use dozer_sql::sqlparser::dialect::GenericDialect;
use dozer_sql::sqlparser::parser::Parser;
use dozer_types::models::connection::Connection;
use dozer_types::types::{Schema, SchemaIdentifier};

use crate::get_schema;
use crate::pipeline::{CacheSinkFactory, ConnectorSourceFactory};

pub struct Executor {}

impl Executor {
    pub fn run<'a>(
        sources: Vec<Source>,
        api_endpoint: ApiEndpoint,
        cache: Arc<LmdbCache>,
    ) -> anyhow::Result<()> {
        let mut source_schemas: Vec<Schema> = vec![];
        let mut connections: Vec<Connection> = vec![];
        let mut table_names: Vec<String> = vec![];

        // Get Source schemas
        let mut idx = 1;
        for source in sources.iter() {
            let schema_tuples = get_schema(source.connection.to_owned())?;

            println!("{:?}", source.table_name);
            let st = schema_tuples
                .iter()
                .find(|t| t.0.eq(&source.table_name))
                .unwrap();

            let schema = st.to_owned().1.clone();
            source_schemas.push(schema);
            connections.push(source.connection.to_owned());
            println!("{:?}", table_names);
            table_names.push(source.table_name.clone());
            idx += 1;
        }

        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

        let ast = Parser::parse_sql(&dialect, &api_endpoint.sql).unwrap();
        println!("AST: {:?}", ast);
        println!("Schemas: {:?}", source_schemas);
        println!("Query: {:?}", &api_endpoint.sql);
        let statement: &Statement = &ast[0];

        let builder = PipelineBuilder::new(source_schemas[0].clone());

        let (mut dag, in_handle, out_handle) =
            builder.statement_to_pipeline(statement.clone()).unwrap();

        let source = ConnectorSourceFactory::new(connections, table_names.clone(), source_schemas);

        // let sink = CacheSinkFactory::new(vec![out_handle.port]);
        let sink = CacheSinkFactory::new(vec![DefaultPortHandle], cache, api_endpoint);

        let source_table_map = source.table_map.clone();

        dag.add_node(NodeType::Source(Box::new(source)), 1.to_string());
        dag.add_node(NodeType::Sink(Box::new(sink)), 4.to_string());

        for (_table_name, endpoint) in in_handle.into_iter() {
            // TODO: Use real table_name
            let table_name = &table_names[0];
            let port = source_table_map.get(table_name).unwrap();
            dag.connect(Endpoint::new(1.to_string(), port.to_owned()), endpoint)?;
        }

        dag.connect(out_handle, Endpoint::new(4.to_string(), DefaultPortHandle))?;

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
