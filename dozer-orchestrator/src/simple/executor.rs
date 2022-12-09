use dozer_api::grpc::internal_grpc::PipelineRequest;
use log::info;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, thread};

use dozer_api::CacheEndpoint;
use dozer_types::models::source::Source;

use crate::pipeline::{CacheSinkFactory, ConnectorSourceFactory};
use dozer_core::dag::dag::{Dag, Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use dozer_core::dag::errors::ExecutionError::{self};
use dozer_core::dag::executor::{DagExecutor, ExecutorOptions};
use dozer_core::dag::node::NodeHandle;
use dozer_ingestion::connectors::TableInfo;
use dozer_ingestion::ingestion::{IngestionIterator, Ingestor};

use dozer_sql::pipeline::builder::PipelineBuilder;
use dozer_sql::sqlparser::ast::Statement;
use dozer_sql::sqlparser::dialect::GenericDialect;
use dozer_sql::sqlparser::parser::Parser;
use dozer_types::crossbeam;
use dozer_types::models::connection::Connection;
use dozer_types::parking_lot::RwLock;

use crate::errors::OrchestrationError;
use crate::validate;

pub struct Executor {
    sources: Vec<Source>,
    cache_endpoints: Vec<CacheEndpoint>,
    home_dir: PathBuf,
    sink_config: SinkConfig,
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
    running: Arc<AtomicBool>,
}
pub struct SinkConfig {
    pub record_cutoff: u32,
    pub timeout: u64,
}
impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            record_cutoff: 50000,
            timeout: 300,
        }
    }
}
impl Executor {
    pub fn new(
        sources: Vec<Source>,
        cache_endpoints: Vec<CacheEndpoint>,
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
        running: Arc<AtomicBool>,
        home_dir: PathBuf,
        sink_config: SinkConfig,
    ) -> Self {
        Self {
            sources,
            cache_endpoints,
            home_dir,
            ingestor,
            iterator,
            running,
            sink_config,
        }
    }

    pub fn run(
        &self,
        notifier: Option<crossbeam::channel::Sender<PipelineRequest>>,
        _running: Arc<AtomicBool>,
    ) -> Result<(), OrchestrationError> {
        let mut connections: Vec<Connection> = vec![];
        let mut connection_map: HashMap<String, Vec<TableInfo>> = HashMap::new();
        let mut table_map: HashMap<String, u16> = HashMap::new();

        // Initialize Source
        // For every pipeline, there will be one Source implementation
        // that can take multiple Connectors on different ports.
        for (table_id, (idx, source)) in self.sources.iter().cloned().enumerate().enumerate() {
            validate(source.connection.to_owned().unwrap())?;

            let table_name = source.table_name.clone();
            let connection = source.connection.to_owned().unwrap();
            let id = match &connection.id {
                Some(id) => id.clone(),
                None => idx.to_string(),
            };
            connections.push(connection);

            let table = TableInfo {
                name: source.table_name,
                id: table_id as u32,
                columns: Some(source.columns),
            };

            connection_map
                .entry(id)
                .and_modify(|v| v.push(table.clone()))
                .or_insert_with(|| vec![table]);

            table_map.insert(table_name, (idx as usize).try_into().unwrap());
        }

        let source_handle = NodeHandle::new(None, "src".to_string());

        let source = ConnectorSourceFactory::new(
            connections,
            connection_map,
            table_map.clone(),
            self.ingestor.to_owned(),
            self.iterator.to_owned(),
        );
        let mut parent_dag = Dag::new();
        parent_dag.add_node(NodeType::Source(Arc::new(source)), source_handle.clone());
        let running_wait = self.running.clone();

        for (idx, cache_endpoint) in self.cache_endpoints.iter().cloned().enumerate() {
            let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

            let api_endpoint = cache_endpoint.endpoint;
            let _api_endpoint_name = api_endpoint.name.clone();
            let cache = cache_endpoint.cache;

            let ast = Parser::parse_sql(&dialect, &api_endpoint.sql).unwrap();

            let statement: &Statement = &ast[0];

            let builder = PipelineBuilder::new(Some(idx as u16));

            let (dag, in_handle, out_handle) = builder
                .statement_to_pipeline(statement.clone())
                .map_err(OrchestrationError::SqlStatementFailed)?;

            parent_dag.merge(Some(idx as u16), dag);

            let sink_handle = NodeHandle::new(Some(idx as u16), "sink".to_string());

            // Initialize Sink
            let sink = CacheSinkFactory::new(
                vec![DEFAULT_PORT_HANDLE],
                cache,
                api_endpoint,
                notifier.clone(),
                self.sink_config.record_cutoff,
                self.sink_config.timeout,
            );
            parent_dag.add_node(NodeType::Sink(Arc::new(sink)), sink_handle.clone());

            // Connect Pipeline to Sink
            parent_dag
                .connect(out_handle, Endpoint::new(sink_handle, DEFAULT_PORT_HANDLE))
                .map_err(OrchestrationError::ExecutionError)?;

            for (table_name, endpoint) in in_handle.into_iter() {
                let port = match table_map.get(&table_name) {
                    Some(port) => Ok(port),
                    None => {
                        info!("Port not found for table_name: {}", table_name);
                        Err(OrchestrationError::PortNotFound(table_name))
                    }
                }?;

                // Connect source from Parent Dag to Processor
                parent_dag
                    .connect(
                        Endpoint::new(source_handle.clone(), port.to_owned()),
                        endpoint,
                    )
                    .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
            }
        }

        let path = self.home_dir.join("pipeline");
        fs::create_dir_all(&path).map_err(|_e| OrchestrationError::InternalServerError)?;
        let exec = DagExecutor::new(&parent_dag, path.as_path(), ExecutorOptions::default())?;

        // Waiting for Ctrl+C
        while running_wait.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(200));
        }

        exec.stop();
        exec.join()
            .map_err(|_e| OrchestrationError::InternalServerError)?;
        Ok(())
    }
}
