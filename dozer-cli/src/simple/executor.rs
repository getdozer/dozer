use dozer_core::shutdown::ShutdownReceiver;
use dozer_tracing::DozerMonitorContext;
use dozer_types::models::flags::Flags;
use dozer_types::models::sink::Sink;
use tokio::runtime::Runtime;

use std::sync::Arc;

use dozer_sql::builder::UdfRegistry;
use dozer_types::models::source::Source;

use crate::pipeline::PipelineBuilder;
use dozer_core::executor::{DagExecutor, ExecutorOptions};

use dozer_types::models::connection::Connection;

use crate::errors::OrchestrationError;

pub struct Executor<'a> {
    connections: &'a [Connection],
    sources: &'a [Source],
    sql: Option<&'a str>,
    sinks: &'a [Sink],
    labels: DozerMonitorContext,
    udfs: UdfRegistry,
}

impl<'a> Executor<'a> {
    // TODO: Refactor this to not require both `contract` and all of
    // connections, sources and sql
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        connections: &'a [Connection],
        sources: &'a [Source],
        sql: Option<&'a str>,
        sinks: &'a [Sink],
        labels: DozerMonitorContext,
        udfs: impl Into<UdfRegistry>,
    ) -> Result<Executor<'a>, OrchestrationError> {
        Ok(Executor {
            connections,
            sources,
            sql,
            sinks,
            labels,
            udfs: udfs.into(),
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
        let exec = DagExecutor::new(dag, executor_options).await?;

        Ok(exec)
    }
}

pub fn run_dag_executor(
    runtime: &Arc<Runtime>,
    dag_executor: DagExecutor,
    shutdown: ShutdownReceiver,
    labels: DozerMonitorContext,
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
