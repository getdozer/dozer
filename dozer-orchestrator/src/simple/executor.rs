use dozer_cache::cache::LmdbCache;
use dozer_core::dag::dag::{Endpoint, NodeType};
use dozer_core::dag::mt_executor::{MultiThreadedDagExecutor, DEFAULT_PORT_HANDLE};
use dozer_sql::pipeline::builder::PipelineBuilder;
use dozer_sql::sqlparser::ast::Statement;
use dozer_sql::sqlparser::dialect::GenericDialect;
use dozer_sql::sqlparser::parser::Parser;
use dozer_types::chk;
use dozer_types::errors::execution::ExecutionError::InternalStringError;
use dozer_types::errors::orchestrator::OrchestrationError;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::connection::Connection;
use dozer_types::models::source::Source;
use dozer_types::test_helper::get_temp_dir;
use dozer_types::types::Schema;
use log::debug;
use rocksdb::{Options, DB};
use std::sync::Arc;

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

        let mut opts = Options::default();
        opts.set_allow_mmap_writes(true);
        opts.optimize_for_point_lookup(1024 * 1024 * 1024);
        opts.set_bytes_per_sync(1024 * 1024 * 10);
        opts.set_manual_wal_flush(true);
        opts.create_if_missing(true);
        let db = chk!(DB::open(&opts, get_temp_dir()));

        exec.start(dag, db)
            .map_err(OrchestrationError::ExecutionError)
    }
}
