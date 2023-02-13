use crate::dag_metadata::DagMetadata;
use crate::dag_schemas::DagSchemas;
use crate::errors::ExecutionError;
use crate::Dag;

use daggy::petgraph::visit::IntoNodeIdentifiers;
use dozer_types::node::NodeHandle;
use dozer_types::types::{Operation, Record};

use crate::epoch::Epoch;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::panic::panic_any;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::thread::{self, Builder};
use std::time::Duration;

#[derive(Clone)]
pub struct ExecutorOptions {
    pub commit_sz: u32,
    pub channel_buffer_sz: usize,
    pub commit_time_threshold: Duration,
}

impl Default for ExecutorOptions {
    fn default() -> Self {
        Self {
            commit_sz: 10_000,
            channel_buffer_sz: 20_000,
            commit_time_threshold: Duration::from_millis(50),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum InputPortState {
    Open,
    Terminated,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutorOperation {
    Delete { old: Record },
    Insert { new: Record },
    Update { old: Record, new: Record },
    Commit { epoch: Epoch },
    Terminate,
}

impl ExecutorOperation {
    pub fn from_operation(op: Operation) -> ExecutorOperation {
        match op {
            Operation::Update { old, new } => ExecutorOperation::Update { old, new },
            Operation::Delete { old } => ExecutorOperation::Delete { old },
            Operation::Insert { new } => ExecutorOperation::Insert { new },
        }
    }
}

impl Display for ExecutorOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let type_str = match self {
            ExecutorOperation::Delete { .. } => "Delete",
            ExecutorOperation::Update { .. } => "Update",
            ExecutorOperation::Insert { .. } => "Insert",
            ExecutorOperation::Terminate { .. } => "Terminate",
            ExecutorOperation::Commit { .. } => "Commit",
        };
        f.write_str(type_str)
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

use self::execution_dag::{ExecutionDag, NodeKind};
use self::source_node::{create_source_nodes, SourceListenerNode, SourceSenderNode};

pub struct DagExecutor<T: Clone> {
    dag_metadata: DagMetadata<T>,
    options: ExecutorOptions,
}

pub struct DagExecutorJoinHandle {
    join_handles: HashMap<NodeHandle, JoinHandle<()>>,
    running: Arc<AtomicBool>,
}

impl<T: Clone + Debug + 'static> DagExecutor<T> {
    pub fn new(
        dag: &Dag<T>,
        path: PathBuf,
        options: ExecutorOptions,
    ) -> Result<Self, ExecutionError> {
        let dag_schemas = DagSchemas::new(dag)?;
        let mut dag_metadata = DagMetadata::new(&dag_schemas, path.clone())?;
        if !dag_metadata.check_consistency() {
            DagMetadata::delete(&path, dag);
            dag_metadata = DagMetadata::new(&dag_schemas, path)?;
            assert!(
                dag_metadata.check_consistency(),
                "We just deleted all metadata"
            );
        }

        Ok(Self {
            dag_metadata,
            options,
        })
    }

    pub fn validate(dag: &Dag<T>, path: PathBuf) -> Result<(), ExecutionError> {
        let dag_schemas = DagSchemas::new(dag)?;
        DagMetadata::new(&dag_schemas, path)?;
        Ok(())
    }

    pub fn start(self, running: Arc<AtomicBool>) -> Result<DagExecutorJoinHandle, ExecutionError> {
        // Construct execution dag.
        let mut execution_dag =
            ExecutionDag::new(self.dag_metadata, self.options.channel_buffer_sz)?;
        let node_indexes = execution_dag.graph().node_identifiers().collect::<Vec<_>>();

        // Start the threads.
        let mut join_handles = HashMap::new();
        for node_index in node_indexes {
            let node = &execution_dag.graph()[node_index];
            let node_handle = node.handle.clone();
            match &node.kind.as_ref().expect("We created all nodes") {
                NodeKind::Source(_, _) => {
                    let (source_sender_node, source_listener_node) = create_source_nodes(
                        &mut execution_dag,
                        node_index,
                        &self.options,
                        running.clone(),
                    );
                    join_handles.insert(
                        node_handle,
                        start_source(source_sender_node, source_listener_node)?,
                    );
                }
                NodeKind::Processor(_) => {
                    let processor_node = ProcessorNode::new(&mut execution_dag, node_index);
                    join_handles.insert(
                        node_handle,
                        start_processor(processor_node, running.clone())?,
                    );
                }
                NodeKind::Sink(_) => {
                    let sink_node = SinkNode::new(&mut execution_dag, node_index);
                    join_handles.insert(node_handle, start_sink(sink_node)?);
                }
            }
        }

        Ok(DagExecutorJoinHandle {
            join_handles,
            running,
        })
    }
}

impl DagExecutorJoinHandle {
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub fn join(mut self) -> Result<(), ExecutionError> {
        let handles: Vec<NodeHandle> = self.join_handles.iter().map(|e| e.0.clone()).collect();

        loop {
            for handle in &handles {
                if let Entry::Occupied(entry) = self.join_handles.entry(handle.clone()) {
                    if entry.get().is_finished() {
                        if let Err(e) = entry.remove().join() {
                            panic_any(e);
                        }
                    }
                }
            }

            if self.join_handles.is_empty() {
                return Ok(());
            }

            thread::sleep(Duration::from_millis(250));
        }
    }
}

fn start_source(
    source_sender: SourceSenderNode,
    source_listener: SourceListenerNode,
) -> Result<JoinHandle<()>, ExecutionError> {
    let handle = source_sender.handle().clone();
    let running = source_sender.running().clone();

    let _st_handle = Builder::new()
        .name(format!("{handle}-sender"))
        .spawn(move || {
            if let Err(e) = source_sender.run() {
                std::panic::panic_any(e);
            }
        })?;

    Ok(Builder::new()
        .name(format!("{handle}-listener"))
        .spawn(move || {
            if let Err(e) = source_listener.run() {
                if running.load(Ordering::Relaxed) {
                    std::panic::panic_any(e);
                }
            }
        })?)
}

fn start_processor(
    processor: ProcessorNode,
    running: Arc<AtomicBool>,
) -> Result<JoinHandle<()>, ExecutionError> {
    Ok(Builder::new()
        .name(processor.handle().to_string())
        .spawn(move || {
            if let Err(e) = processor.run() {
                if running.load(Ordering::Relaxed) {
                    std::panic::panic_any(e);
                }
            }
        })?)
}

fn start_sink(sink: SinkNode) -> Result<JoinHandle<()>, ExecutionError> {
    Ok(Builder::new().name(sink.handle().to_string()).spawn(|| {
        if let Err(e) = sink.run() {
            std::panic::panic_any(e);
        }
    })?)
}
