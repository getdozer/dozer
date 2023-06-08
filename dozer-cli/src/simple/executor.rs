use dozer_cache::dozer_log::home_dir::HomeDir;
use dozer_types::models::api_endpoint::ApiEndpoint;
use tokio::runtime::Runtime;

use std::collections::HashMap;
use std::path::PathBuf;
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
    /// `ApiEndpoint` and its log path.
    endpoint_and_log_paths: Vec<(ApiEndpoint, PathBuf)>,
    running: Arc<AtomicBool>,
    multi_pb: MultiProgress,
}
impl<'a> Executor<'a> {
    pub fn new(
        home_dir: &'a HomeDir,
        connections: &'a [Connection],
        sources: &'a [Source],
        sql: Option<&'a str>,
        api_endpoints: &'a [ApiEndpoint],
        running: Arc<AtomicBool>,
        multi_pb: MultiProgress,
    ) -> Result<Self, OrchestrationError> {
        let mut endpoint_and_log_paths = vec![];
        for endpoint in api_endpoints {
            let migration_path = home_dir
                .find_latest_migration_path(&endpoint.name)
                .map_err(|(path, error)| OrchestrationError::FileSystem(path, error))?
                .ok_or(OrchestrationError::NoMigrationFound(endpoint.name.clone()))?;
            endpoint_and_log_paths.push((endpoint.clone(), migration_path.log_path));
        }

        Ok(Self {
            connections,
            sources,
            sql,
            endpoint_and_log_paths,
            running,
            multi_pb,
        })
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
    ) -> Result<DagExecutor, OrchestrationError> {
        let builder = PipelineBuilder::new(
            self.connections,
            self.sources,
            self.sql,
            self.endpoint_and_log_paths.clone(),
            self.multi_pb.clone(),
        );

        let dag = builder.build(runtime, settings)?;
        let exec = DagExecutor::new(dag, executor_options)?;

        Ok(exec)
    }

    pub fn run_dag_executor(&self, dag_executor: DagExecutor) -> Result<(), OrchestrationError> {
        let join_handle = dag_executor.start(self.running.clone())?;
        join_handle.join().map_err(ExecutionError)
    }
}
