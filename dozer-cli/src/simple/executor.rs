use dozer_api::grpc::internal::internal_pipeline_server::LogEndpoint;
use dozer_cache::dozer_log::home_dir::{BuildPath, HomeDir};
use dozer_cache::dozer_log::replication::{Log, LogOptions};
use dozer_types::models::api_endpoint::ApiEndpoint;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use std::sync::Arc;

use dozer_types::models::source::Source;
use dozer_types::models::udf_config::UdfConfig;

use crate::pipeline::PipelineBuilder;
use crate::shutdown::ShutdownReceiver;
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
    endpoint_and_logs: Vec<(ApiEndpoint, LogEndpoint)>,
    multi_pb: MultiProgress,
    udfs: &'a [UdfConfig],
}

impl<'a> Executor<'a> {
    #[ignore = "clippy::too_many_arguments"]
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
        let build_path = home_dir
            .find_latest_build_path()
            .map_err(|(path, error)| OrchestrationError::FileSystem(path.into(), error))?
            .ok_or(OrchestrationError::NoBuildFound)?;
        let mut endpoint_and_logs = vec![];
        for endpoint in api_endpoints {
            let log_endpoint =
                create_log_endpoint(&build_path, &endpoint.name, log_options.clone()).await?;
            endpoint_and_logs.push((endpoint.clone(), log_endpoint));
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

    pub fn endpoint_and_logs(&self) -> &[(ApiEndpoint, LogEndpoint)] {
        &self.endpoint_and_logs
    }

    pub async fn create_dag_executor(
        &self,
        runtime: &Arc<Runtime>,
        executor_options: ExecutorOptions,
        shutdown: ShutdownReceiver,
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

        let dag = builder.build(runtime, shutdown).await?;
        let exec = DagExecutor::new(dag, executor_options)?;

        Ok(exec)
    }
}

pub fn run_dag_executor(
    dag_executor: DagExecutor,
    shutdown: ShutdownReceiver,
) -> Result<(), OrchestrationError> {
    let join_handle = dag_executor.start(shutdown.get_running_flag())?;
    join_handle.join().map_err(ExecutionError)
}

async fn create_log_endpoint(
    build_path: &BuildPath,
    endpoint_name: &str,
    log_options: LogOptions,
) -> Result<LogEndpoint, OrchestrationError> {
    let endpoint_path = build_path.get_endpoint_path(endpoint_name);

    let schema_string = tokio::fs::read_to_string(&endpoint_path.schema_path)
        .await
        .map_err(|e| OrchestrationError::FileSystem(endpoint_path.schema_path.into(), e))?;

    let descriptor_bytes = tokio::fs::read(&build_path.descriptor_path)
        .await
        .map_err(|e| {
            OrchestrationError::FileSystem(build_path.descriptor_path.clone().into(), e)
        })?;

    let log = Log::new(log_options, endpoint_path.log_dir.into_string(), false).await?;
    let log = Arc::new(Mutex::new(log));

    Ok(LogEndpoint {
        build_id: build_path.id.clone(),
        schema_string,
        descriptor_bytes,
        log,
    })
}
