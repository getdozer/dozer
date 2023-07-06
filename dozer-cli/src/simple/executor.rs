use dozer_cache::dozer_log::home_dir::HomeDir;
use dozer_cache::dozer_log::replication::{self, Log};
use dozer_cache::dozer_log::storage::{LocalStorage, Storage};
use dozer_types::models::api_endpoint::ApiEndpoint;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use dozer_types::models::source::Source;

use crate::pipeline::PipelineBuilder;
use dozer_core::executor::{DagExecutor, ExecutorOptions};

use dozer_types::indicatif::MultiProgress;

use dozer_types::models::connection::Connection;
use OrchestrationError::ExecutionError;

use crate::errors::OrchestrationError;

pub struct Executor<'a, S: Storage> {
    connections: &'a [Connection],
    sources: &'a [Source],
    sql: Option<&'a str>,
    /// `ApiEndpoint` and its log path.
    endpoint_and_logs: Vec<(ApiEndpoint, Arc<Mutex<Log<S>>>)>,
    running: Arc<AtomicBool>,
    multi_pb: MultiProgress,
}

impl<'a, S: Storage + Debug> Executor<'a, S> {
    pub async fn new_with_local_storage(
        home_dir: &'a HomeDir,
        connections: &'a [Connection],
        sources: &'a [Source],
        sql: Option<&'a str>,
        api_endpoints: &'a [ApiEndpoint],
        running: Arc<AtomicBool>,
        log_entry_max_size: usize,
        multi_pb: MultiProgress,
    ) -> Result<Executor<'a, LocalStorage>, OrchestrationError> {
        let mut endpoint_and_logs = vec![];
        for endpoint in api_endpoints {
            let migration_path = home_dir
                .find_latest_migration_path(&endpoint.name)
                .map_err(|(path, error)| OrchestrationError::FileSystem(path.into(), error))?
                .ok_or(OrchestrationError::NoMigrationFound(endpoint.name.clone()))?;
            let storage = LocalStorage::new(migration_path.log_path.to_string())
                .await
                .map_err(replication::Error::Storage)?;
            let log = Log::new(storage, "".into(), false, log_entry_max_size).await?;
            let log = Arc::new(Mutex::new(log));
            endpoint_and_logs.push((endpoint.clone(), log));
        }

        Ok(Executor {
            connections,
            sources,
            sql,
            endpoint_and_logs,
            running,
            multi_pb,
        })
    }

    pub fn create_dag_executor(
        &self,
        runtime: Arc<Runtime>,
        executor_options: ExecutorOptions,
    ) -> Result<DagExecutor, OrchestrationError> {
        let builder = PipelineBuilder::new(
            self.connections,
            self.sources,
            self.sql,
            self.endpoint_and_logs
                .iter()
                .map(|(endpoint, log)| (endpoint.clone(), Some(log.clone())))
                .collect(),
            self.multi_pb.clone(),
        );

        let dag = builder.build(runtime)?;
        let exec = DagExecutor::new(dag, executor_options)?;

        Ok(exec)
    }

    pub fn run_dag_executor(&self, dag_executor: DagExecutor) -> Result<(), OrchestrationError> {
        let join_handle = dag_executor.start(self.running.clone())?;
        join_handle.join().map_err(ExecutionError)
    }
}
