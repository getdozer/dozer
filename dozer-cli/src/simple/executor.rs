use dozer_api::grpc::internal::internal_pipeline_server::LogEndpoint;
use dozer_api::shutdown::ShutdownReceiver;
use dozer_cache::dozer_log::camino::Utf8Path;
use dozer_cache::dozer_log::home_dir::{BuildPath, HomeDir};
use dozer_cache::dozer_log::replication::Log;
use dozer_core::checkpoint::{CheckpointOptions, OptionCheckpoint};
use dozer_tracing::LabelsAndProgress;
use dozer_types::models::endpoint::{Endpoint, EndpointKind};
use dozer_types::models::flags::Flags;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use std::sync::{atomic::AtomicBool, Arc};

use dozer_types::models::source::Source;
use dozer_types::models::udf_config::UdfConfig;

use crate::pipeline::{EndpointLog, EndpointLogKind, PipelineBuilder};
use dozer_core::executor::{DagExecutor, ExecutorOptions};

use dozer_types::models::connection::Connection;

use crate::errors::{BuildError, OrchestrationError};

use super::Contract;

pub struct Executor<'a> {
    connections: &'a [Connection],
    sources: &'a [Source],
    sql: Option<&'a str>,
    checkpoint: OptionCheckpoint,
    endpoints: Vec<ExecutorEndpoint>,
    labels: LabelsAndProgress,
    udfs: &'a [UdfConfig],
}

#[derive(Debug)]
struct ExecutorEndpoint {
    table_name: String,
    kind: ExecutorEndpointKind,
}

#[derive(Debug)]
enum ExecutorEndpointKind {
    Api { log_endpoint: LogEndpoint },
    Dummy,
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
        endpoints: &'a [Endpoint],
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

        let mut executor_endpoints = vec![];
        for endpoint in endpoints {
            let kind = match &endpoint.kind {
                EndpointKind::Api(_) => {
                    let log_endpoint = create_log_endpoint(
                        contract,
                        &build_path,
                        &endpoint.table_name,
                        &checkpoint,
                    )
                    .await?;
                    ExecutorEndpointKind::Api { log_endpoint }
                }
                EndpointKind::Dummy => ExecutorEndpointKind::Dummy,
            };

            executor_endpoints.push(ExecutorEndpoint {
                table_name: endpoint.table_name.clone(),
                kind,
            });
        }

        Ok(Executor {
            connections,
            sources,
            sql,
            checkpoint,
            endpoints: executor_endpoints,
            labels,
            udfs,
        })
    }

    pub fn checkpoint_prefix(&self) -> &str {
        self.checkpoint.prefix()
    }

    pub fn table_name_and_logs(&self) -> Vec<(String, LogEndpoint)> {
        self.endpoints
            .iter()
            .filter_map(|endpoint| {
                if let ExecutorEndpointKind::Api { log_endpoint, .. } = &endpoint.kind {
                    Some((endpoint.table_name.clone(), log_endpoint.clone()))
                } else {
                    None
                }
            })
            .collect()
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
            self.endpoints
                .into_iter()
                .map(|endpoint| {
                    let kind = match endpoint.kind {
                        ExecutorEndpointKind::Api { log_endpoint, .. } => EndpointLogKind::Api {
                            log: log_endpoint.log,
                        },
                        ExecutorEndpointKind::Dummy => EndpointLogKind::Dummy,
                    };
                    EndpointLog {
                        table_name: endpoint.table_name,
                        kind,
                    }
                })
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
    table_name: &str,
    checkpoint: &OptionCheckpoint,
) -> Result<LogEndpoint, OrchestrationError> {
    let endpoint_path = build_path.get_endpoint_path(table_name);

    let schema = contract
        .endpoints
        .get(table_name)
        .ok_or_else(|| BuildError::MissingEndpoint(table_name.to_owned()))?;
    let schema_string =
        dozer_types::serde_json::to_string(schema).map_err(BuildError::SerdeJson)?;

    let log_prefix = AsRef::<Utf8Path>::as_ref(checkpoint.prefix())
        .join(&endpoint_path.log_dir_relative_to_data_dir);
    let log = Log::new(
        checkpoint.storage(),
        log_prefix.into(),
        checkpoint.last_epoch_id(),
    )
    .await?;
    let log = Arc::new(Mutex::new(log));

    Ok(LogEndpoint { schema_string, log })
}
