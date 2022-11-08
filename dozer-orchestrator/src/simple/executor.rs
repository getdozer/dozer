use crate::errors::OrchestrationError;
use dozer_api::CacheEndpoint;
use dozer_ingestion::ingestion::{IngestionIterator, Ingestor};
use dozer_types::crossbeam;
use dozer_types::events::Event;
use dozer_types::parking_lot::RwLock;
use log::info;
use std::collections::HashMap;
use std::fs;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use dozer_types::models::source::Source;
use tempdir::TempDir;

use crate::pipeline::{CacheSinkFactory, ConnectorSourceFactory};
use dozer_core::dag::dag::{Dag, Endpoint, NodeType};
use dozer_core::dag::errors::ExecutionError::{self};
use dozer_core::dag::executor_local::{MultiThreadedDagExecutor, DEFAULT_PORT_HANDLE};
use dozer_sql::pipeline::builder::PipelineBuilder;
use dozer_sql::sqlparser::ast::Statement;
use dozer_sql::sqlparser::dialect::GenericDialect;
use dozer_sql::sqlparser::parser::Parser;
use dozer_types::models::connection::Connection;

pub struct Executor {
    sources: Vec<Source>,
    cache_endpoints: Vec<CacheEndpoint>,
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
}

impl Executor {
    pub fn new(
        sources: Vec<Source>,
        cache_endpoints: Vec<CacheEndpoint>,
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
    ) -> Self {
        Self {
            sources,
            cache_endpoints,
            ingestor,
            iterator,
        }
    }

    pub fn run(
        &self,
        notifier: Option<crossbeam::channel::Sender<Event>>,
        running: Arc<AtomicBool>,
    ) -> Result<(), OrchestrationError> {
        let mut connections: Vec<Connection> = vec![];
        let mut connection_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut table_map: HashMap<String, u16> = HashMap::new();

        // Initialize Source
        // For every pipeline, there will be one Source implementation
        // that can take multiple Connectors on different ports.
        for (idx, source) in self.sources.iter().cloned().enumerate() {
            let table_name = source.table_name.clone();
            let connection = source.connection.to_owned();
            let id = match &connection.id {
                Some(id) => id.clone(),
                None => idx.to_string(),
            };
            connections.push(connection);

            let table_names = match connection_map.get(&id) {
                Some(v) => {
                    let mut v = v.clone();
                    v.push(table_name.clone());
                    v
                }
                None => vec![source.table_name.clone()],
            };
            connection_map.insert(id, table_names);
            table_map.insert(table_name, (idx as usize).try_into().unwrap());
        }

        let source = ConnectorSourceFactory::new(
            connections,
            connection_map,
            table_map.clone(),
            running,
            self.ingestor.to_owned(),
            self.iterator.to_owned(),
        );
        let mut parent_dag = Dag::new();
        parent_dag.add_node(NodeType::Source(Box::new(source)), 1.to_string());

        for (idx, cache_endpoint) in self.cache_endpoints.iter().cloned().enumerate() {
            let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

            let api_endpoint = cache_endpoint.endpoint;
            let api_endpoint_name = api_endpoint.name.clone();
            let cache = cache_endpoint.cache;

            let ast = Parser::parse_sql(&dialect, &api_endpoint.sql).unwrap();

            let statement: &Statement = &ast[0];

            let builder = PipelineBuilder {};

            let (mut dag, in_handle, out_handle) = builder
                .statement_to_pipeline(statement.clone())
                .map_err(OrchestrationError::SqlStatementFailed)?;

            // Initialize Sink
            let sink = CacheSinkFactory::new(
                vec![DEFAULT_PORT_HANDLE],
                cache,
                api_endpoint,
                notifier.clone(),
            );
            dag.add_node(NodeType::Sink(Box::new(sink)), 4.to_string());

            // Connect Pipeline to Sink
            dag.connect(
                out_handle,
                Endpoint::new(4.to_string(), DEFAULT_PORT_HANDLE),
            )
            .map_err(OrchestrationError::ExecutionError)?;

            let namespace = format!("{}_{}", api_endpoint_name, idx);
            // Merge dag with parent with namespace
            parent_dag.merge(namespace.to_owned(), dag);

            for (table_name, endpoint) in in_handle.into_iter() {
                let port = match table_map.get(&table_name) {
                    Some(port) => Ok(port),
                    None => {
                        info!("Port not found for table_name: {}", table_name);
                        Err(OrchestrationError::PortNotFound(table_name))
                    }
                }?;

                // Change namespace after merge
                let mut endpoint = endpoint.clone();
                endpoint.node = format!("{}_{}", namespace, endpoint.node);

                // Connect source from Parent Dag to Processor
                parent_dag
                    .connect(Endpoint::new(1.to_string(), port.to_owned()), endpoint)
                    .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
            }
        }

        let exec = MultiThreadedDagExecutor::new(100000);

        let tmp_dir =
            TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
        if tmp_dir.path().exists() {
            fs::remove_dir_all(tmp_dir.path())
                .unwrap_or_else(|_e| panic!("Unable to remove old dir"));
        }
        fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

        exec.start(parent_dag, tmp_dir.into_path())
            .map_err(OrchestrationError::ExecutionError)
    }
}
