use dozer_core::checkpoint::{CheckpointOptions, OptionCheckpoint};
use dozer_core::shutdown::ShutdownReceiver;
use dozer_log::home_dir::HomeDir;
use dozer_tracing::LabelsAndProgress;
use dozer_types::models::flags::Flags;
use dozer_types::models::sink::Sink;
use tokio::runtime::Runtime;

use std::sync::Arc;

use dozer_types::models::source::Source;
use dozer_types::models::udf_config::UdfConfig;

use crate::pipeline::PipelineBuilder;
use dozer_core::executor::{DagExecutor, ExecutorOptions};

use dozer_types::models::connection::Connection;

use crate::errors::OrchestrationError;

pub struct Executor<'a> {
    connections: &'a [Connection],
    sources: &'a [Source],
    sql: Option<&'a str>,
    checkpoint: OptionCheckpoint,
    sinks: &'a [Sink],
    labels: LabelsAndProgress,
    udfs: &'a [UdfConfig],
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
        sinks: &'a [Sink],
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

        Ok(Executor {
            connections,
            sources,
            sql,
            checkpoint,
            sinks,
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
            self.sinks,
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
