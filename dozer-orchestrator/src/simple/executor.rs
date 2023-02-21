use dozer_api::grpc::internal_grpc::PipelineResponse;
use dozer_cache::cache::CacheManagerOptions;
use dozer_core::app::{App, AppPipeline};
use dozer_sql::pipeline::builder::{statement_to_pipeline, SchemaSQLContext};
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::app_config::Config;
use dozer_types::types::{Operation, SourceSchema};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use dozer_types::models::source::Source;

use crate::pipeline::validate::validate;
use crate::pipeline::{CacheSinkSettings, PipelineBuilder, StreamingSinkFactory};
use dozer_core::executor::{DagExecutor, ExecutorOptions};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_ingestion::connectors::get_connector;

use dozer_types::crossbeam;

use dozer_types::models::connection::Connection;
use OrchestrationError::ExecutionError;

use crate::errors::OrchestrationError;
use crate::pipeline::source_builder::SourceBuilder;

pub struct Executor {
    config: Config,
    api_endpoints: Vec<ApiEndpoint>,
    pipeline_dir: PathBuf,
    running: Arc<AtomicBool>,
}
impl Executor {
    pub fn new(
        config: Config,
        api_endpoints: Vec<ApiEndpoint>,
        running: Arc<AtomicBool>,
        pipeline_dir: PathBuf,
    ) -> Self {
        Self {
            config,
            api_endpoints,
            pipeline_dir,
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
    ) -> Result<dozer_core::Dag<SchemaSQLContext>, OrchestrationError> {
        let grouped_connections = self.get_connection_groups();

        let mut pipeline = AppPipeline::new();
        let transform_response = statement_to_pipeline(&sql, &mut pipeline, None)
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
        let asm = source_builder.build_source_manager()?;

        let mut app = App::new(asm);
        app.add_pipeline(pipeline);

        let dag = app.get_dag().map_err(OrchestrationError::ExecutionError)?;
        let exec = DagExecutor::new(&dag, self.pipeline_dir.clone(), ExecutorOptions::default())?;

        exec.start(self.running.clone())?;
        Ok(dag)
    }

    pub fn get_tables(
        connections: &Vec<Connection>,
    ) -> Result<HashMap<String, Vec<SourceSchema>>, OrchestrationError> {
        let mut schema_map = HashMap::new();
        for connection in connections {
            validate(connection.to_owned(), None)?;

            let connector = get_connector(connection.to_owned())?;
            let schema_tuples = connector.get_schemas(None)?;
            schema_map.insert(connection.name.to_owned(), schema_tuples);
        }

        Ok(schema_map)
    }

    pub fn create_dag_executor(
        &self,
        notifier: Option<crossbeam::channel::Sender<PipelineResponse>>,
        cache_manager_options: CacheManagerOptions,
        settings: CacheSinkSettings,
        executor_options: ExecutorOptions,
    ) -> Result<DagExecutor, OrchestrationError> {
        let builder = PipelineBuilder::new(
            self.config.clone(),
            self.api_endpoints.clone(),
            self.pipeline_dir.clone(),
        );

        let dag = builder.build(notifier, cache_manager_options, settings)?;
        let path = &self.pipeline_dir;

        if !path.exists() {
            return Err(OrchestrationError::PipelineDirectoryNotFound(
                path.to_string_lossy().to_string(),
            ));
        }

        let exec = DagExecutor::new(&dag, path.to_path_buf(), executor_options)?;

        Ok(exec)
    }

    pub fn run_dag_executor(&self, dag_executor: DagExecutor) -> Result<(), OrchestrationError> {
        let join_handle = dag_executor.start(self.running.clone())?;
        join_handle.join().map_err(ExecutionError)
    }
}
