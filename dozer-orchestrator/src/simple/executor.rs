use dozer_types::errors::orchestrator::OrchestrationError;
use log::debug;
use std::fs;
use std::sync::Arc;

use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::source::Source;
use tempdir::TempDir;

use dozer_cache::cache::LmdbCache;
use dozer_core::dag::dag::{Endpoint, NodeType};
use dozer_core::dag::mt_executor::{MultiThreadedDagExecutor, DEFAULT_PORT_HANDLE};
use dozer_core::state::lmdb::LmdbStateStoreManager;
use dozer_sql::pipeline::builder::PipelineBuilder;
use dozer_sql::sqlparser::ast::Statement;
use dozer_sql::sqlparser::dialect::GenericDialect;
use dozer_sql::sqlparser::parser::Parser;
use dozer_types::errors::execution::ExecutionError::InternalStringError;
use dozer_types::models::connection::Connection;
use dozer_types::types::Schema;

use crate::get_schema;
use crate::pipeline::{CacheSinkFactory, ConnectorSourceFactory};

pub struct Executor {}

impl Executor {
    pub fn run(
        sources: Vec<Source>,
        api_endpoint: ApiEndpoint,
        cache: Arc<LmdbCache>,
        schema_change_notifier: crossbeam::channel::Sender<bool>,
    ) -> Result<(), OrchestrationError> {
        let mut source_schemas: Vec<Schema> = vec![];
        let mut connections: Vec<Connection> = vec![];
        let mut table_names: Vec<String> = vec![];

        // Get Source schemas
        for source in sources.iter() {
            let schema_tuples = get_schema(source.connection.to_owned())
                .map_err(|e| InternalStringError(e.to_string()))?;

            debug!("{:?}", source.table_name);
            let st = schema_tuples
                .iter()
                .find(|t| t.0.eq(&source.table_name))
                .unwrap();

            let schema = st.to_owned().1.clone();
            source_schemas.push(schema);
            connections.push(source.connection.to_owned());
            table_names.push(source.table_name.clone());
        }

        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

        let ast = Parser::parse_sql(&dialect, &api_endpoint.sql).unwrap();
        debug!("AST: {:?}", ast);
        debug!("Schemas: {:?}", source_schemas);
        debug!("Query: {:?}", &api_endpoint.sql);
        let statement: &Statement = &ast[0];

        let builder = PipelineBuilder {};

        let (mut dag, in_handle, out_handle) =
            builder.statement_to_pipeline(statement.clone()).unwrap();

        let source = ConnectorSourceFactory::new(connections, table_names.clone(), source_schemas);

        // let sink = CacheSinkFactory::new(vec![out_handle.port]);
        let sink = CacheSinkFactory::new(
            vec![DEFAULT_PORT_HANDLE],
            cache,
            api_endpoint,
            schema_change_notifier,
        );

        let source_table_map = source.table_map.clone();

        dag.add_node(NodeType::Source(Box::new(source)), 1.to_string());
        dag.add_node(NodeType::Sink(Box::new(sink)), 4.to_string());

        for (_table_name, endpoint) in in_handle.into_iter() {
            // TODO: Use real table_name
            let table_name = &table_names[0];
            let port = source_table_map.get(table_name).unwrap();
            dag.connect(Endpoint::new(1.to_string(), port.to_owned()), endpoint)
                .map_err(|e| InternalStringError(e.to_string()))?;
        }

        dag.connect(
            out_handle,
            Endpoint::new(4.to_string(), DEFAULT_PORT_HANDLE),
        )
        .map_err(OrchestrationError::ExecutionError)?;

        let exec = MultiThreadedDagExecutor::new(100000);

        let tmp_dir =
            TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
        if tmp_dir.path().exists() {
            fs::remove_dir_all(tmp_dir.path())
                .unwrap_or_else(|_e| panic!("Unable to remove old dir"));
        }
        fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

        let sm = Arc::new(LmdbStateStoreManager::new(
            tmp_dir.path().to_str().unwrap().to_string(),
            1024 * 1024 * 1024 * 5,
            20_000,
        ));
        exec.start(dag, sm)
            .map_err(OrchestrationError::ExecutionError)
    }
}
