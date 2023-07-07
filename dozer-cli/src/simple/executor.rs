use dozer_cache::dozer_log::home_dir::HomeDir;
use dozer_cache::dozer_log::replication::{Log, LogOptions};
use dozer_types::models::api_endpoint::ApiEndpoint;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use dozer_types::models::source::Source;

use crate::pipeline::PipelineBuilder;
use dozer_core::executor::{DagExecutor, ExecutorOptions};

use dozer_types::indicatif::MultiProgress;

use dozer_types::models::connection::Connection;
use OrchestrationError::ExecutionError;

use crate::errors::OrchestrationError;

pub struct Executor<'a> {
    connections: &'a [Connection],
    sources: &'a [Source],
    sql: Option<&'a str>,
    /// `ApiEndpoint` and its log path.
    endpoint_and_logs: Vec<(ApiEndpoint, Arc<Mutex<Log>>)>,
    multi_pb: MultiProgress,
}

impl<'a> Executor<'a> {
    pub async fn new(
        home_dir: &'a HomeDir,
        connections: &'a [Connection],
        sources: &'a [Source],
        sql: Option<&'a str>,
        api_endpoints: &'a [ApiEndpoint],
        log_options: LogOptions,
        multi_pb: MultiProgress,
    ) -> Result<Executor<'a>, OrchestrationError> {
        let mut endpoint_and_logs = vec![];
        for endpoint in api_endpoints {
            let build_path = home_dir
                .find_latest_build_path(&endpoint.name)
                .map_err(|(path, error)| OrchestrationError::FileSystem(path.into(), error))?
                .ok_or(OrchestrationError::NoBuildFound(endpoint.name.clone()))?;
            let log = Log::new(log_options.clone(), &build_path, false).await?;
            let log = Arc::new(Mutex::new(log));
            endpoint_and_logs.push((endpoint.clone(), log));
        }

        Ok(Executor {
            connections,
            sources,
            sql,
            endpoint_and_logs,
            multi_pb,
        })
    }

    pub fn endpoint_and_logs(&self) -> &[(ApiEndpoint, Arc<Mutex<Log>>)] {
        &self.endpoint_and_logs
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
}

pub fn run_dag_executor(
    dag_executor: DagExecutor,
    running: Arc<AtomicBool>,
) -> Result<(), OrchestrationError> {
    let join_handle = dag_executor.start(running)?;
    join_handle.join().map_err(ExecutionError)
}
