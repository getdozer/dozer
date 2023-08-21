use crate::builder_dag::{BuilderDag, NodeKind};
use crate::dag_schemas::DagSchemas;
use crate::errors::ExecutionError;
use crate::Dag;

use daggy::petgraph::visit::IntoNodeIdentifiers;

use dozer_types::serde::{self, Deserialize, Serialize};
use std::fmt::Debug;
use std::panic::panic_any;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::thread::{self, Builder};
use std::time::Duration;

#[derive(Clone)]
pub struct ExecutorOptions {
    pub commit_sz: u32,
    pub channel_buffer_sz: usize,
    pub commit_time_threshold: Duration,
    pub error_threshold: Option<u32>,
}

impl Default for ExecutorOptions {
    fn default() -> Self {
        Self {
            commit_sz: 10_000,
            channel_buffer_sz: 20_000,
            commit_time_threshold: Duration::from_millis(50),
            error_threshold: Some(0),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
pub(crate) enum InputPortState {
    Open,
    Terminated,
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
use self::source_node::{create_source_nodes, SourceListenerNode, SourceSenderNode};

pub struct DagExecutor {
    builder_dag: BuilderDag,
    options: ExecutorOptions,
}

pub struct DagExecutorJoinHandle {
    join_handles: Vec<JoinHandle<()>>,
}

impl DagExecutor {
    pub fn new<T: Clone + Debug>(
        dag: Dag<T>,
        options: ExecutorOptions,
    ) -> Result<Self, ExecutionError> {
        let dag_schemas = DagSchemas::new(dag)?;
        let builder_dag = BuilderDag::new(dag_schemas)?;

        Ok(Self {
            builder_dag,
            options,
        })
    }

    pub fn validate<T: Clone + Debug>(dag: Dag<T>) -> Result<(), ExecutionError> {
        DagSchemas::new(dag)?;
        Ok(())
    }

    pub fn start(self, running: Arc<AtomicBool>) -> Result<DagExecutorJoinHandle, ExecutionError> {
        // Construct execution dag.
        let mut execution_dag = ExecutionDag::new(
            self.builder_dag,
            self.options.channel_buffer_sz,
            self.options.error_threshold,
        )?;
        let node_indexes = execution_dag.graph().node_identifiers().collect::<Vec<_>>();

        // Start the threads.
        let mut join_handles = Vec::new();
        for node_index in node_indexes {
            let node = execution_dag.graph()[node_index]
                .as_ref()
                .expect("We created all nodes");
            match &node.kind {
                NodeKind::Source(_, _) => {
                    let (source_sender_node, source_listener_node) = create_source_nodes(
                        &mut execution_dag,
                        node_index,
                        &self.options,
                        running.clone(),
                    );
                    let (sender, receiver) =
                        start_source(source_sender_node, source_listener_node)?;
                    join_handles.extend([sender, receiver]);
                }
                NodeKind::Processor(_) => {
                    let processor_node = ProcessorNode::new(&mut execution_dag, node_index);
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
            if let Err(e) = handle.join() {
                panic_any(e)
            }

            if self.join_handles.is_empty() {
                return Ok(());
            }
        }
    }
}

fn start_source(
    source_sender: SourceSenderNode,
    source_listener: SourceListenerNode,
) -> Result<(JoinHandle<()>, JoinHandle<()>), ExecutionError> {
    let handle = source_sender.handle().clone();

    let sender_handle = Builder::new()
        .name(format!("{handle}-sender"))
        .spawn(move || match source_sender.run() {
            Ok(_) => {}
            // Channel disconnection means the source listener has quit.
            // Maybe it quit gracefully so we don't need to panic.
            Err(e) => {
                if let ExecutionError::Source(e) = &e {
                    if let Some(ExecutionError::CannotSendToChannel) = e.downcast_ref() {
                        return;
                    }
                }
                std::panic::panic_any(e);
            }
        })
        .map_err(ExecutionError::CannotSpawnWorkerThread)?;

    let listener_handle = Builder::new()
        .name(format!("{handle}-listener"))
        .spawn(move || {
            if let Err(e) = source_listener.run() {
                std::panic::panic_any(e);
            }
        })
        .map_err(ExecutionError::CannotSpawnWorkerThread)?;

    Ok((sender_handle, listener_handle))
}

fn start_processor(processor: ProcessorNode) -> Result<JoinHandle<()>, ExecutionError> {
    Builder::new()
        .name(processor.handle().to_string())
        .spawn(move || {
            if let Err(e) = processor.run() {
                std::panic::panic_any(e);
            }
        })
        .map_err(ExecutionError::CannotSpawnWorkerThread)
}

fn start_sink(sink: SinkNode) -> Result<JoinHandle<()>, ExecutionError> {
    Builder::new()
        .name(sink.handle().to_string())
        .spawn(|| {
            if let Err(e) = sink.run() {
                std::panic::panic_any(e);
            }
        })
        .map_err(ExecutionError::CannotSpawnWorkerThread)
}
