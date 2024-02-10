use dozer_cache::dozer_log::home_dir::HomeDir;
use dozer_core::checkpoint::{CheckpointOptions, OptionCheckpoint};
use dozer_core::shutdown::ShutdownReceiver;
use dozer_tracing::LabelsAndProgress;
use dozer_types::models::endpoint::{
    AerospikeSinkConfig, ClickhouseSinkConfig, Endpoint, EndpointKind, OracleSinkConfig,
};
use dozer_types::models::flags::Flags;
use tokio::runtime::Runtime;

use std::sync::Arc;

use dozer_types::models::source::Source;
use dozer_types::models::udf_config::UdfConfig;

use crate::pipeline::{EndpointLog, EndpointLogKind, PipelineBuilder};
use dozer_core::executor::{DagExecutor, ExecutorOptions};

use dozer_types::models::connection::Connection;

use crate::errors::OrchestrationError;

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
    Dummy,
    Aerospike { config: AerospikeSinkConfig },
    Clickhouse { config: ClickhouseSinkConfig },
    Oracle { config: OracleSinkConfig },
}

impl<'a> Executor<'a> {
    // TODO: Refactor this to not require both `contract` and all of
    // connections, sources and sql
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        home_dir: &'a HomeDir,
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
            let kind = match &endpoint.config {
                EndpointKind::Dummy => ExecutorEndpointKind::Dummy,
                EndpointKind::Aerospike(config) => ExecutorEndpointKind::Aerospike {
                    config: config.clone(),
                },
                EndpointKind::Oracle(config) => ExecutorEndpointKind::Oracle {
                    config: config.clone(),
                },
                EndpointKind::Clickhouse(config) => ExecutorEndpointKind::Clickhouse {
                    config: config.clone(),
                },
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
                        ExecutorEndpointKind::Dummy => EndpointLogKind::Dummy,
                        ExecutorEndpointKind::Aerospike { config } => {
                            EndpointLogKind::Aerospike { config }
                        }
                        ExecutorEndpointKind::Clickhouse { config } => {
                            EndpointLogKind::Clickhouse { config }
                        }
                        ExecutorEndpointKind::Oracle { config } => {
                            EndpointLogKind::Oracle { config }
                        }
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
    runtime: &Arc<Runtime>,
    dag_executor: DagExecutor,
    shutdown: ShutdownReceiver,
    labels: LabelsAndProgress,
) -> Result<(), OrchestrationError> {
    let join_handle = runtime.block_on(dag_executor.start(
        Box::pin(shutdown.create_shutdown_future()),
        labels,
        runtime.clone(),
    ))?;
    join_handle
        .join()
        .map_err(OrchestrationError::ExecutionError)
}
