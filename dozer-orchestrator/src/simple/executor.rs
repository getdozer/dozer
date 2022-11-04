use crate::errors::OrchestrationError;
use dozer_api::CacheEndpoint;
use dozer_types::crossbeam;
use log::debug;
use std::fs;

use dozer_types::models::source::Source;
use tempdir::TempDir;

use dozer_core::dag::dag::{Endpoint, NodeType};
use dozer_core::dag::errors::ExecutionError::{self};
use dozer_core::dag::mt_executor::{MultiThreadedDagExecutor, DEFAULT_PORT_HANDLE};
use dozer_sql::pipeline::builder::PipelineBuilder;
use dozer_sql::sqlparser::ast::Statement;
use dozer_sql::sqlparser::dialect::GenericDialect;
use dozer_sql::sqlparser::parser::Parser;
use dozer_types::models::connection::Connection;
use dozer_types::types::Schema;

use crate::get_single_schema;
use crate::pipeline::{CacheSinkFactory, ConnectorSourceFactory};

pub struct Executor {}

impl Executor {
    pub fn run(
        sources: Vec<Source>,
        cache_endpoint: CacheEndpoint,
        schema_change_notifier: crossbeam::channel::Sender<bool>,
    ) -> Result<(), OrchestrationError> {
        let mut source_schemas: Vec<Schema> = vec![];
        let mut connections: Vec<Connection> = vec![];
        let mut table_names: Vec<String> = vec![];

        // Get Source schemas
        for source in sources.iter() {
            let schema = get_single_schema(source.connection.to_owned(), source.table_name.clone())
                .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

            source_schemas.push(schema);
            connections.push(source.connection.to_owned());
            table_names.push(source.table_name.clone());
        }

        let source = ConnectorSourceFactory::new(connections, table_names.clone(), source_schemas);
        let source_table_map = source.table_map.clone();

        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

        let api_endpoint = cache_endpoint.endpoint;
        let cache = cache_endpoint.cache;

        let ast = Parser::parse_sql(&dialect, &api_endpoint.sql).unwrap();

        let statement: &Statement = &ast[0];

        let builder = PipelineBuilder {};

        let (mut dag, in_handle, out_handle) =
            builder.statement_to_pipeline(statement.clone()).unwrap();

        // let sink = CacheSinkFactory::new(vec![out_handle.port]);
        let sink = CacheSinkFactory::new(
            vec![DEFAULT_PORT_HANDLE],
            cache,
            api_endpoint,
            schema_change_notifier,
        );

        dag.add_node(NodeType::Source(Box::new(source)), 1.to_string());
        dag.add_node(NodeType::Sink(Box::new(sink)), (4).to_string());

        for (_table_name, endpoint) in in_handle.into_iter() {
            // TODO: Use real table_name
            let table_name = &table_names[0];
            let port = source_table_map.get(table_name).unwrap();
            dag.connect(Endpoint::new(1.to_string(), port.to_owned()), endpoint)
                .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
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

        exec.start(dag, tmp_dir.into_path())
            .map_err(OrchestrationError::ExecutionError)
    }
}
