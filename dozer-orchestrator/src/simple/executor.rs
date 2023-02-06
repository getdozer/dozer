use dozer_api::grpc::internal_grpc::PipelineResponse;
use dozer_core::dag::app::{App, AppPipeline};
use dozer_sql::pipeline::builder::{statement_to_pipeline, SchemaSQLContext};
use dozer_types::models::app_config::Config;
use dozer_types::types::{Operation, SchemaWithChangesType};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use dozer_api::CacheEndpoint;
use dozer_types::models::source::Source;

use crate::pipeline::validate::validate;
use crate::pipeline::{CacheSinkSettings, PipelineBuilder, StreamingSinkFactory};
use dozer_core::dag::executor::{DagExecutor, ExecutorOptions};
use dozer_core::dag::DEFAULT_PORT_HANDLE;
use dozer_ingestion::connectors::get_connector;

use dozer_ingestion::ingestion::{IngestionIterator, Ingestor};

use dozer_types::crossbeam;

use dozer_types::models::connection::Connection;
use dozer_types::parking_lot::RwLock;
use OrchestrationError::ExecutionError;

use crate::errors::OrchestrationError;
use crate::pipeline::source_builder::SourceBuilder;

pub struct Executor {
    config: Config,
    cache_endpoints: Vec<CacheEndpoint>,
    pipeline_dir: PathBuf,
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
    running: Arc<AtomicBool>,
}
impl Executor {
    pub fn new(
        config: Config,
        cache_endpoints: Vec<CacheEndpoint>,
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
        running: Arc<AtomicBool>,
        pipeline_dir: PathBuf,
    ) -> Self {
        Self {
            config,
            cache_endpoints,
            pipeline_dir,
            ingestor,
            iterator,
            running,
        }
    }

    pub fn get_connection_groups(&self) -> HashMap<String, Vec<Source>> {
        SourceBuilder::group_connections(self.config.sources.clone())
    }

    // This function is used to run a query using a temporary pipeline
    pub fn query(
        &self,
        sql: String,
        sender: crossbeam::channel::Sender<Operation>,
    ) -> Result<dozer_core::dag::Dag<SchemaSQLContext>, OrchestrationError> {
        let grouped_connections = self.get_connection_groups();

        let mut pipeline = AppPipeline::new();
        let transform_response = statement_to_pipeline(&sql, &mut pipeline)
            .map_err(OrchestrationError::PipelineError)?;
        pipeline.add_sink(
            Arc::new(StreamingSinkFactory::new(sender)),
            "streaming_sink",
        );

        let table_info = transform_response
            .output_tables_map
            .values()
            .next()
            .unwrap();
        pipeline
            .connect_nodes(
                &table_info.node,
                Some(table_info.port),
                "streaming_sink",
                Some(DEFAULT_PORT_HANDLE),
                true,
            )
            .map_err(OrchestrationError::ExecutionError)?;

        let used_sources: Vec<String> = pipeline.get_entry_points_sources_names();

        let source_builder = SourceBuilder::new(used_sources, grouped_connections);
        let asm = source_builder.build_source_manager(
            self.ingestor.clone(),
            self.iterator.clone(),
            self.running.clone(),
        )?;
        let mut app = App::new(asm);
        app.add_pipeline(pipeline);

        let dag = app.get_dag().map_err(OrchestrationError::ExecutionError)?;
        let path = &self.pipeline_dir;
        let mut exec = DagExecutor::new(
            &dag,
            path.as_path(),
            ExecutorOptions::default(),
            self.running.clone(),
        )?;

        exec.start()?;
        Ok(dag)
    }

    pub fn get_tables(
        connections: &Vec<Connection>,
    ) -> Result<HashMap<String, Vec<SchemaWithChangesType>>, OrchestrationError> {
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
        settings: CacheSinkSettings,
    ) -> Result<(), OrchestrationError> {
        let running_wait = self.running.clone();

        let builder = PipelineBuilder::new(
            self.config.clone(),
            self.cache_endpoints.clone(),
            self.ingestor.clone(),
            self.iterator.clone(),
            self.running.clone(),
            self.pipeline_dir.clone(),
        );

        let parent_dag = builder.build(notifier, PathBuf::default(), settings)?;
        let path = &self.pipeline_dir;

        if !path.exists() {
            return Err(OrchestrationError::PipelineDirectoryNotFound(
                path.to_string_lossy().to_string(),
            ));
        }

        let mut exec = DagExecutor::new(
            &parent_dag,
            path.as_path(),
            ExecutorOptions::default(),
            running_wait,
        )?;

        exec.start()?;
        exec.join().map_err(ExecutionError)
    }
}
