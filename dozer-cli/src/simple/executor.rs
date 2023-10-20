use dozer_api::grpc::internal::internal_pipeline_server::LogEndpoint;
use dozer_api::shutdown::ShutdownReceiver;
use dozer_cache::dozer_log::camino::Utf8Path;
use dozer_cache::dozer_log::home_dir::{BuildPath, HomeDir};
use dozer_cache::dozer_log::replication::Log;
use dozer_core::checkpoint::{CheckpointOptions, OptionCheckpoint};
use dozer_tracing::LabelsAndProgress;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::flags::Flags;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use std::sync::{atomic::AtomicBool, Arc};

use dozer_types::models::source::Source;
use dozer_types::models::udf_config::UdfConfig;

use crate::pipeline::PipelineBuilder;
use dozer_core::executor::{DagExecutor, ExecutorOptions};

use dozer_types::models::connection::Connection;

use crate::errors::{BuildError, OrchestrationError};

use super::Contract;

pub struct Executor<'a> {
    connections: &'a [Connection],
    sources: &'a [Source],
    sql: Option<&'a str>,
    checkpoint: OptionCheckpoint,
    /// `ApiEndpoint` and its log.
    endpoint_and_logs: Vec<(ApiEndpoint, LogEndpoint)>,
    labels: LabelsAndProgress,
    udfs: &'a [UdfConfig],
}

impl<'a> Executor<'a> {
    // TODO: Refactor this to not require both `contract` and all of
    // connections, sources and sql
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        home_dir: &'a HomeDir,
        contract: &Contract,
        connections: &'a [Connection],
        sources: &'a [Source],
        sql: Option<&'a str>,
        api_endpoints: &'a [ApiEndpoint],
        checkpoint_options: CheckpointOptions,
        labels: LabelsAndProgress,
        udfs: &'a [UdfConfig],
    ) -> Result<Executor<'a>, OrchestrationError> {
        // Find the build path.
        let build_path = home_dir
            .find_latest_build_path()
            .map_err(|(path, error)| OrchestrationError::FileSystem(path.into(), error))?
            .ok_or(OrchestrationError::NoBuildFound)?;

        // Load pipeline checkpoint.
        let checkpoint =
            OptionCheckpoint::new(build_path.data_dir.to_string(), checkpoint_options).await?;

        let mut endpoint_and_logs = vec![];
        for endpoint in api_endpoints {
            let log_endpoint = create_log_endpoint(
                contract,
                &build_path,
                &endpoint.name,
                &checkpoint,
                checkpoint.num_slices(),
            )
            .await?;
            endpoint_and_logs.push((endpoint.clone(), log_endpoint));
        }

        Ok(Executor {
            connections,
            sources,
            sql,
            checkpoint,
            endpoint_and_logs,
            labels,
            udfs,
        })
    }

    pub fn endpoint_and_logs(&self) -> &[(ApiEndpoint, LogEndpoint)] {
        &self.endpoint_and_logs
    }

    pub async fn create_dag_executor(
        self,
        runtime: &Arc<Runtime>,
        executor_options: ExecutorOptions,
        shutdown: ShutdownReceiver,
        flags: Flags,
    ) -> Result<DagExecutor, OrchestrationError> {
        let builder = PipelineBuilder::new(
            self.connections,
            self.sources,
            self.sql,
            self.endpoint_and_logs
                .into_iter()
                .map(|(endpoint, log)| (endpoint, Some(log.log)))
                .collect(),
            self.labels.clone(),
            flags,
            self.udfs,
        );

        let dag = builder.build(runtime, shutdown).await?;
        let exec = DagExecutor::new(dag, self.checkpoint, executor_options).await?;

        Ok(exec)
    }
}

pub fn run_dag_executor(
    runtime: &Runtime,
    dag_executor: DagExecutor,
    running: Arc<AtomicBool>,
    labels: LabelsAndProgress,
) -> Result<(), OrchestrationError> {
    let join_handle = runtime.block_on(dag_executor.start(running, labels))?;
    join_handle
        .join()
        .map_err(OrchestrationError::ExecutionError)
}

async fn create_log_endpoint(
    contract: &Contract,
    build_path: &BuildPath,
    endpoint_name: &str,
    checkpoint: &OptionCheckpoint,
    num_persisted_entries_to_keep: usize,
) -> Result<LogEndpoint, OrchestrationError> {
    let endpoint_path = build_path.get_endpoint_path(endpoint_name);

    let schema = contract
        .endpoints
        .get(endpoint_name)
        .ok_or_else(|| BuildError::MissingEndpoint(endpoint_name.to_owned()))?;
    let schema_string =
        dozer_types::serde_json::to_string(schema).map_err(BuildError::SerdeJson)?;

    let log_prefix = AsRef::<Utf8Path>::as_ref(checkpoint.prefix())
        .join(&endpoint_path.log_dir_relative_to_data_dir);
    let log = Log::new(
        checkpoint.storage(),
        log_prefix.into(),
        num_persisted_entries_to_keep,
    )
    .await?;
    let log = Arc::new(Mutex::new(log));

    Ok(LogEndpoint {
        build_id: build_path.id.clone(),
        schema_string,
        log,
    })
}
