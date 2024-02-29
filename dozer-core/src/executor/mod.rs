use crate::builder_dag::{BuilderDag, NodeKind};
use crate::dag_schemas::DagSchemas;
use crate::errors::ExecutionError;
use crate::Dag;

use daggy::petgraph::visit::IntoNodeIdentifiers;

use dozer_tracing::LabelsAndProgress;
use futures::Future;
use std::fmt::Debug;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::thread::{self, Builder};
use std::time::Duration;
use tokio::runtime::Runtime;

#[derive(Debug, Clone)]
pub struct ExecutorOptions {
    pub channel_buffer_sz: usize,
    pub event_hub_capacity: usize,
    pub error_threshold: Option<u32>,
}

impl Default for ExecutorOptions {
    fn default() -> Self {
        Self {
            channel_buffer_sz: 20_000,
            event_hub_capacity: 100,
            error_threshold: Some(0),
        }
    }
}

mod execution_dag;
mod name;
mod node;
mod processor_node;
mod receiver_loop;
mod sink_node;
mod source_node;

use node::Node;
use processor_node::ProcessorNode;
use sink_node::SinkNode;

use self::execution_dag::ExecutionDag;
use self::source_node::{create_source_node, SourceNode};

pub struct DagExecutor {
    builder_dag: BuilderDag,
    options: ExecutorOptions,
}

pub struct DagExecutorJoinHandle {
    join_handles: Vec<JoinHandle<Result<(), ExecutionError>>>,
}

impl DagExecutor {
    pub async fn new(dag: Dag, options: ExecutorOptions) -> Result<Self, ExecutionError> {
        let dag_schemas = DagSchemas::new(dag).await?;

        let builder_dag = BuilderDag::new(dag_schemas, options.event_hub_capacity).await?;

        Ok(Self {
            builder_dag,
            options,
        })
    }

    pub async fn validate<T: Clone + Debug>(dag: Dag) -> Result<(), ExecutionError> {
        DagSchemas::new(dag).await?;
        Ok(())
    }

    pub async fn start<F: Send + 'static + Future + Unpin>(
        self,
        shutdown: F,
        labels: LabelsAndProgress,
        runtime: Arc<Runtime>,
    ) -> Result<DagExecutorJoinHandle, ExecutionError> {
        // Construct execution dag.
        let mut execution_dag = ExecutionDag::new(
            self.builder_dag,
            labels,
            self.options.channel_buffer_sz,
            self.options.error_threshold,
        )?;
        let node_indexes = execution_dag.graph().node_identifiers().collect::<Vec<_>>();

        // Start the threads.
        let source_node =
            create_source_node(&mut execution_dag, &self.options, shutdown, runtime.clone()).await;
        let mut join_handles = vec![start_source(source_node)?];
        for node_index in node_indexes {
            let Some(node) = execution_dag.graph()[node_index].kind.as_ref() else {
                continue;
            };
            match &node {
                NodeKind::Source { .. } => unreachable!("We already started the source node"),
                NodeKind::Processor(_) => {
                    let processor_node = ProcessorNode::new(&mut execution_dag, node_index).await;
                    join_handles.push(start_processor(processor_node)?);
                }
                NodeKind::Sink(_) => {
                    let sink_node = SinkNode::new(&mut execution_dag, node_index);
                    join_handles.push(start_sink(sink_node)?);
                }
            }
        }

        Ok(DagExecutorJoinHandle { join_handles })
    }
}

impl DagExecutorJoinHandle {
    pub fn join(mut self) -> Result<(), ExecutionError> {
        loop {
            let Some(finished) = self
                .join_handles
                .iter()
                .enumerate()
                .find_map(|(i, handle)| handle.is_finished().then_some(i))
            else {
                thread::sleep(Duration::from_millis(250));

                continue;
            };
            let handle = self.join_handles.swap_remove(finished);
            handle.join().unwrap()?;

            if self.join_handles.is_empty() {
                return Ok(());
            }
        }
    }
}

fn start_source<F: Send + 'static + Future + Unpin>(
    source: SourceNode<F>,
) -> Result<JoinHandle<Result<(), ExecutionError>>, ExecutionError> {
    let handle = Builder::new()
        .name("sources".into())
        .spawn(move || match source.run() {
            Ok(()) => Ok(()),
            // Channel disconnection means the source listener has quit.
            // Maybe it quit gracefully so we don't need to propagate the error.
            Err(e) => {
                if let ExecutionError::Source(e) = &e {
                    if let Some(ExecutionError::CannotSendToChannel) = e.downcast_ref() {
                        return Ok(());
                    }
                }
                Err(e)
            }
        })
        .map_err(ExecutionError::CannotSpawnWorkerThread)?;

    Ok(handle)
}

fn start_processor(
    processor: ProcessorNode,
) -> Result<JoinHandle<Result<(), ExecutionError>>, ExecutionError> {
    Builder::new()
        .name(processor.handle().to_string())
        .spawn(move || {
            processor.run()?;
            Ok(())
        })
        .map_err(ExecutionError::CannotSpawnWorkerThread)
}

fn start_sink(sink: SinkNode) -> Result<JoinHandle<Result<(), ExecutionError>>, ExecutionError> {
    Builder::new()
        .name(sink.handle().to_string())
        .spawn(|| {
            sink.run()?;
            Ok(())
        })
        .map_err(ExecutionError::CannotSpawnWorkerThread)
}
