use dozer_api::grpc::internal::internal_pipeline_server::BuildAndLog;
use dozer_cache::dozer_log::home_dir::HomeDir;
use dozer_cache::dozer_log::replication::{Log, LogOptions};
use dozer_types::models::api_endpoint::ApiEndpoint;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use dozer_types::models::source::Source;
use dozer_types::models::udf_config::UdfConfig;

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
    /// `ApiEndpoint` and its log.
    endpoint_and_logs: Vec<(ApiEndpoint, BuildAndLog)>,
    multi_pb: MultiProgress,
    udfs: &'a [UdfConfig],
}

#[ignore(clippy::too_many_arguments)]
impl<'a> Executor<'a> {
    pub async fn new(
        home_dir: &'a HomeDir,
        connections: &'a [Connection],
        sources: &'a [Source],
        sql: Option<&'a str>,
        api_endpoints: &'a [ApiEndpoint],
        log_options: LogOptions,
        multi_pb: MultiProgress,
        udfs: &'a [UdfConfig],
    ) -> Result<Executor<'a>, OrchestrationError> {
        let mut endpoint_and_logs = vec![];
        for endpoint in api_endpoints {
            let build_path = home_dir
                .find_latest_build_path(&endpoint.name)
                .map_err(|(path, error)| OrchestrationError::FileSystem(path.into(), error))?
                .ok_or(OrchestrationError::NoBuildFound(endpoint.name.clone()))?;
            let log = Log::new(log_options.clone(), &build_path, false).await?;
            let log = Arc::new(Mutex::new(log));
            endpoint_and_logs.push((
                endpoint.clone(),
                BuildAndLog {
                    build: build_path,
                    log,
                },
            ));
        }

        Ok(Executor {
            connections,
            sources,
            sql,
            endpoint_and_logs,
            multi_pb,
            udfs,
        })
    }

    pub fn endpoint_and_logs(&self) -> &[(ApiEndpoint, BuildAndLog)] {
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
                .map(|(endpoint, log)| (endpoint.clone(), Some(log.log.clone())))
                .collect(),
            self.multi_pb.clone(),
            self.udfs,
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
