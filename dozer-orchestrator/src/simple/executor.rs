use dozer_api::grpc::internal_grpc::PipelineResponse;
use dozer_core::dag::app::App;
use dozer_types::types::Schema;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use std::fs;

use dozer_api::CacheEndpoint;
use dozer_types::models::source::Source;

use crate::pipeline::{CacheSinkFactory, ConnectorSourceFactory};
use dozer_core::dag::dag::{Dag, NodeType, DEFAULT_PORT_HANDLE};
use dozer_core::dag::executor::{DagExecutor, ExecutorOptions};
use dozer_core::dag::node::NodeHandle;
use dozer_ingestion::connectors::{get_connector, TableInfo};
use dozer_ingestion::ingestion::{IngestionIterator, Ingestor};

use dozer_sql::pipeline::builder::PipelineBuilder;
use dozer_sql::sqlparser::dialect::GenericDialect;
use dozer_types::crossbeam;
use dozer_types::models::connection::Connection;
use dozer_types::parking_lot::RwLock;

use crate::errors::OrchestrationError;
use crate::validate;

pub struct Executor {
    sources: Vec<Source>,
    cache_endpoints: Vec<CacheEndpoint>,
    home_dir: PathBuf,
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
    running: Arc<AtomicBool>,
}
impl Executor {
    pub fn new(
        sources: Vec<Source>,
        cache_endpoints: Vec<CacheEndpoint>,
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
        running: Arc<AtomicBool>,
        home_dir: PathBuf,
    ) -> Self {
        Self {
            sources,
            cache_endpoints,
            home_dir,
            ingestor,
            iterator,
            running,
        }
    }
    pub fn get_tables(
        connections: &Vec<Connection>,
    ) -> Result<HashMap<String, Vec<(String, Schema)>>, OrchestrationError> {
        let mut schema_map = HashMap::new();
        for connection in connections {
            validate(connection.to_owned())?;

            let connector = get_connector(connection.to_owned())?;
            let schema_tuples = connector.get_schemas(None)?;
            schema_map.insert(connection.name.to_owned(), schema_tuples);
        }

        Ok(schema_map)
    }

    #[allow(clippy::type_complexity)]
    pub fn get_connection_map(
        sources: &[Source],
    ) -> Result<(HashMap<Connection, Vec<TableInfo>>, HashMap<String, u16>), OrchestrationError>
    {
        let mut connection_map: HashMap<Connection, Vec<TableInfo>> = HashMap::new();
        let mut table_map: HashMap<String, u16> = HashMap::new();

        // Initialize Source
        // For every pipeline, there will be one Source implementation
        // that can take multiple Connectors on different ports.
        for (table_id, (idx, source)) in sources.iter().cloned().enumerate().enumerate() {
            validate(source.connection.to_owned().unwrap())?;

            let table_name = source.table_name.clone();
            let connection = source
                .connection
                .expect("connection is expected")
                .to_owned();
            let table = TableInfo {
                name: source.table_name,
                id: table_id as u32,
                columns: Some(source.columns),
            };

            connection_map
                .entry(connection)
                .and_modify(|v| v.push(table.clone()))
                .or_insert_with(|| vec![table]);

            table_map.insert(table_name, idx.try_into().unwrap());
        }
        Ok((connection_map, table_map))
    }

    pub fn run(
        &self,
        notifier: Option<crossbeam::channel::Sender<PipelineResponse>>,
    ) -> Result<(), OrchestrationError> {
        let source_handle = NodeHandle::new(None, "src".to_string());

        let (connection_map, table_map) = Self::get_connection_map(&self.sources)?;
        let source = ConnectorSourceFactory::new(
            connection_map,
            table_map.clone(),
            self.ingestor.to_owned(),
            self.iterator.to_owned(),
        );
        let running_wait = self.running.clone();

        let asm = SourceBuilder::build_source_manager(
            self.sources.clone(),
            self.ingestor.clone(),
            self.iterator.clone(),
        );
        let mut app = App::new(asm);

        for cache_endpoint in self.cache_endpoints.iter().cloned() {
            let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

            let api_endpoint = cache_endpoint.endpoint.clone();
            let _api_endpoint_name = api_endpoint.name.clone();
            let cache = cache_endpoint.cache;

            let mut pipeline = PipelineBuilder {}
                .build_pipeline(&api_endpoint.sql)
                .map_err(OrchestrationError::SqlStatementFailed)?;

            pipeline.add_sink(
                Arc::new(CacheSinkFactory::new(
                    vec![DEFAULT_PORT_HANDLE],
                    cache,
                    api_endpoint,
                    notifier,
                )),
                cache_endpoint.endpoint.id(),
            );

            pipeline
                .connect_nodes(
                    "aggregation",
                    Some(DEFAULT_PORT_HANDLE),
                    cache_endpoint.endpoint.id(),
                    Some(DEFAULT_PORT_HANDLE),
                )
                .map_err(OrchestrationError::ExecutionError)?;

            app.add_pipeline(pipeline);
        }

        let parent_dag = app.get_dag().map_err(OrchestrationError::ExecutionError)?;

        let path = &self.home_dir;
        fs::create_dir_all(path).map_err(|_e| OrchestrationError::InternalServerError)?;
        let mut exec = DagExecutor::new(
            &parent_dag,
            path.as_path(),
            ExecutorOptions::default(),
            running_wait,
        )?;

        exec.start()?;

        exec.join().map_err(OrchestrationError::ExecutionError)
    }
}
