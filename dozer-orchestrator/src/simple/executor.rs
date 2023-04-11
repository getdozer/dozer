use dozer_types::models::api_endpoint::ApiEndpoint;
use tokio::runtime::Runtime;

use dozer_api::grpc::internal::internal_pipeline_server::PipelineEventSenders;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use dozer_types::models::source::Source;

use crate::pipeline::{LogSinkSettings, PipelineBuilder};
use dozer_core::executor::{DagExecutor, ExecutorOptions};

use dozer_ingestion::connectors::{get_connector, SourceSchema, TableInfo};
use dozer_types::indicatif::MultiProgress;

use dozer_types::models::connection::Connection;
use OrchestrationError::ExecutionError;

use crate::errors::OrchestrationError;

pub struct Executor<'a> {
    connections: &'a [Connection],
    sources: &'a [Source],
    sql: Option<&'a str>,
    api_endpoints: &'a [ApiEndpoint],
    pipeline_dir: &'a Path,
    running: Arc<AtomicBool>,
    multi_pb: MultiProgress,
}
impl<'a> Executor<'a> {
    pub fn new(
        connections: &'a [Connection],
        sources: &'a [Source],
        sql: Option<&'a str>,
        api_endpoints: &'a [ApiEndpoint],
        pipeline_dir: &'a Path,
        running: Arc<AtomicBool>,
        multi_pb: MultiProgress,
    ) -> Self {
        Self {
            connections,
            sources,
            sql,
            api_endpoints,
            pipeline_dir,
            running,
            multi_pb,
        }
    }

    #[allow(clippy::type_complexity)]
    pub async fn get_tables(
        connections: &Vec<Connection>,
    ) -> Result<HashMap<String, (Vec<TableInfo>, Vec<SourceSchema>)>, OrchestrationError> {
        let mut schema_map = HashMap::new();
        for connection in connections {
            let connector = get_connector(connection.to_owned())?;
            let schema_tuples = connector.list_all_schemas().await?;
            schema_map.insert(connection.name.to_owned(), schema_tuples);
        }

        Ok(schema_map)
    }

    pub fn create_dag_executor(
        &self,
        runtime: Arc<Runtime>,
        settings: LogSinkSettings,
        executor_options: ExecutorOptions,
        notifier: Option<PipelineEventSenders>,
    ) -> Result<DagExecutor, OrchestrationError> {
        let builder = PipelineBuilder::new(
            self.connections,
            self.sources,
            self.sql,
            self.api_endpoints,
            self.pipeline_dir,
            self.multi_pb.clone(),
        );

        let dag = builder.build(runtime, settings, notifier)?;
        let path = &self.pipeline_dir;

        if !path.exists() {
            return Err(OrchestrationError::PipelineDirectoryNotFound(
                path.to_string_lossy().to_string(),
            ));
        }

        let exec = DagExecutor::new(dag, path.to_path_buf(), executor_options)?;

        Ok(exec)
    }

    pub fn run_dag_executor(&self, dag_executor: DagExecutor) -> Result<(), OrchestrationError> {
        let join_handle = dag_executor.start(self.running.clone())?;
        join_handle.join().map_err(ExecutionError)
    }
}
