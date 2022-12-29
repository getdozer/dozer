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

use crate::pipeline::CacheSinkFactory;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::executor::{DagExecutor, ExecutorOptions};
use dozer_ingestion::connectors::{get_connector, TableInfo};
use dozer_ingestion::ingestion::{IngestionIterator, Ingestor};

use dozer_sql::pipeline::builder::PipelineBuilder;
use dozer_types::crossbeam;
use dozer_types::models::connection::Connection;
use dozer_types::parking_lot::RwLock;

use crate::errors::OrchestrationError;
use crate::pipeline::source_builder::SourceBuilder;
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
            validate(connection.to_owned(), None)?;

            let connector = get_connector(connection.to_owned())?;
            let schema_tuples = connector.get_schemas(None)?;
            schema_map.insert(connection.name.to_owned(), schema_tuples);
        }

        Ok(schema_map)
    }

    pub fn run(
        &self,
        notifier: Option<crossbeam::channel::Sender<PipelineResponse>>,
    ) -> Result<(), OrchestrationError> {
        let grouped_connections = SourceBuilder::group_connections(self.sources.clone());
        for sources_group in grouped_connections.values() {
            let first_source = sources_group.get(0).unwrap();

            if let Some(connection) = &first_source.connection {
                let tables = sources_group
                    .iter()
                    .map(|source| TableInfo {
                        name: source.table_name.clone(),
                        id: 0,
                        columns: Some(source.columns.clone()),
                    })
                    .collect();

                validate(connection.clone(), Some(tables))?;
            }
        }
        let running_wait = self.running.clone();

        let asm = SourceBuilder::build_source_manager(
            grouped_connections,
            self.ingestor.clone(),
            self.iterator.clone(),
        );
        let mut app = App::new(asm);

        for cache_endpoint in self.cache_endpoints.iter().cloned() {
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
                    notifier.clone(),
                )),
                cache_endpoint.endpoint.name.as_str(),
            );

            pipeline
                .connect_nodes(
                    "aggregation",
                    Some(DEFAULT_PORT_HANDLE),
                    cache_endpoint.endpoint.name.as_str(),
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
