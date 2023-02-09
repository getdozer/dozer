use crate::dag_metadata::{Consistency, DagMetadata, DagMetadataManager};
use crate::dag_schemas::{DagSchemas, NodeSchemas};
use crate::errors::ExecutionError;
use crate::errors::ExecutionError::{IncompatibleSchemas, InconsistentCheckpointMetadata};
use crate::node::NodeHandle;
use crate::Dag;

use dozer_types::types::{Operation, Record};

use crate::epoch::Epoch;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::panic::panic_any;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
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

use self::execution_dag::ExecutionDag;
use self::source_node::{create_source_nodes, SourceListenerNode, SourceSenderNode};

use super::epoch::OpIdentifier;

pub struct DagExecutor<T: Clone> {
    dag: Dag<T>,
    join_handles: HashMap<NodeHandle, JoinHandle<()>>,
    path: PathBuf,
    options: ExecutorOptions,
    running: Arc<AtomicBool>,
    consistency_metadata: HashMap<NodeHandle, Option<OpIdentifier>>,
}

impl<T: Clone + Debug + 'static> DagExecutor<T> {
    fn check_consistency(
        dag: &Dag<T>,
        path: &Path,
    ) -> Result<HashMap<NodeHandle, Option<OpIdentifier>>, ExecutionError> {
        let mut r = HashMap::new();
        let meta = DagMetadataManager::new(dag, path)?;
        let chk = meta.get_checkpoint_consistency()?;
        for (handle, _factory) in dag.sources() {
            match chk.get(handle) {
                Some(Consistency::FullyConsistent(c)) => {
                    r.insert(handle.clone(), *c);
                }
                _ => return Err(InconsistentCheckpointMetadata),
            }
        }
        Ok(r)
    }

    pub fn new(
        dag: Dag<T>,
        path: &Path,
        options: ExecutorOptions,
        running: Arc<AtomicBool>,
    ) -> Result<Self, ExecutionError> {
        //

        let consistency_metadata = match Self::check_consistency(&dag, path) {
            Ok(c) => c,
            Err(_) => {
                DagMetadataManager::new(&dag, path)?.delete_metadata();
                dag.sources()
                    .map(|(handle, _)| (handle.clone(), None))
                    .collect()
            }
        };

        Self::load_or_init_schema(&dag, path)?;

        Ok(Self {
            dag,
            path: path.to_path_buf(),
            join_handles: HashMap::new(),
            options,
            running,
            consistency_metadata,
        })
    }

    pub fn validate(dag: &Dag<T>, path: &Path) -> Result<(), ExecutionError> {
        let dag_schemas = DagSchemas::new(dag)?;
        let meta_manager = DagMetadataManager::new(dag, path)?;

        let current_schemas = dag_schemas.get_all_schemas();
        let existing_schemas = meta_manager.get_metadata()?;
        for (handle, current) in &current_schemas {
            if let Some(existing) = existing_schemas.get(handle) {
                Self::validate_schemas(current, existing)?;
            } else {
                // Non-existing schemas is OK. `Executor::new` will initialize the schemas.
            }
        }

        Ok(())
    }

    fn validate_schemas(
        current: &NodeSchemas<T>,
        existing: &DagMetadata,
    ) -> Result<(), ExecutionError> {
        if existing.output_schemas.len() != current.output_schemas.len() {
            return Err(IncompatibleSchemas(
                "Output Schemas length mismatch".to_string(),
            ));
        }
        for (port, (schema, _ctx)) in &current.output_schemas {
            let other_schema = existing
                .output_schemas
                .get(port)
                .ok_or(IncompatibleSchemas(format!(
                    "Cannot find output schema on port {port:?}"
                )))?;
            if schema != other_schema {
                schema.print().printstd();

                other_schema.print().printstd();
                return Err(IncompatibleSchemas(format!(
                    "Schema mismatch for port {port:?}:"
                )));
            }
        }
        if existing.input_schemas.len() != current.input_schemas.len() {
            return Err(IncompatibleSchemas(
                "Input Schemas length mismatch".to_string(),
            ));
        }
        for (port, (schema, _ctx)) in &current.input_schemas {
            let other_schema =
                existing
                    .input_schemas
                    .get(port)
                    .ok_or(IncompatibleSchemas(format!(
                        "Cannot find input schema on port {port:?}",
                    )))?;
            if schema != other_schema {
                schema.print().printstd();

                other_schema.print().printstd();
                return Err(IncompatibleSchemas(format!(
                    "Schema mismatch for port {port:?}:",
                )));
            }
        }
        Ok(())
    }

    fn load_or_init_schema(
        dag: &Dag<T>,
        path: &Path,
    ) -> Result<HashMap<NodeHandle, NodeSchemas<T>>, ExecutionError> {
        let dag_schemas = DagSchemas::new(dag)?;
        let meta_manager = DagMetadataManager::new(dag, path)?;

        let current_schemas = dag_schemas.get_all_schemas();
        match meta_manager.get_metadata() {
            Ok(existing_schemas) => {
                for (handle, current) in &current_schemas {
                    if let Some(existing) = existing_schemas.get(handle) {
                        Self::validate_schemas(current, existing)?;
                    } else {
                        meta_manager.delete_metadata();
                        meta_manager.init_metadata(&current_schemas)?;
                    }
                }
            }
            Err(_) => {
                meta_manager.delete_metadata();
                meta_manager.init_metadata(&current_schemas)?;
            }
        };

        Ok(current_schemas)
    }

    pub fn start(&mut self) -> Result<(), ExecutionError> {
        // Construct execution dag.
        let dag_schemas = DagSchemas::new(&self.dag)?;
        let mut execution_dag =
            ExecutionDag::new(&dag_schemas, &self.path, self.options.channel_buffer_sz)?;

        // Create source nodes.
        let source_nodes = self
            .dag
            .source_identifiers()
            .map(|source_index| {
                let node_handle = &self.dag.graph()[source_index].handle;
                let checkpoint = self
                    .consistency_metadata
                    .get(node_handle)
                    .ok_or_else(|| ExecutionError::InvalidNodeHandle(node_handle.clone()))?;

                Ok::<_, ExecutionError>(create_source_nodes(
                    &mut execution_dag,
                    source_index,
                    &self.options,
                    *checkpoint,
                    self.running.clone(),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Create processor and sink nodes.
        let processor_nodes = self
            .dag
            .processor_identifiers()
            .map(|processor_index| ProcessorNode::new(&mut execution_dag, processor_index))
            .collect::<Vec<_>>();
        let sink_nodes = self
            .dag
            .sink_identifiers()
            .map(|sink_index| SinkNode::new(&mut execution_dag, sink_index))
            .collect::<Vec<_>>();

        // Drop execution DAG because it holds some of the senders and receivers.
        drop(execution_dag);

        // Start sinks.
        for sink in sink_nodes {
            let handle = sink.handle().clone();
            let join_handle = start_sink(sink)?;
            self.join_handles.insert(handle, join_handle);
        }

        // Start processors.
        for processor in processor_nodes {
            let handle = processor.handle().clone();
            let join_handle = start_processor(processor, self.running.clone())?;
            self.join_handles.insert(handle, join_handle);
        }

        // Start sources.
        let num_sources = self.dag.sources().count();
        let start_barrier = Arc::new(Barrier::new(num_sources));

        for (source_sender_node, source_listener_node) in source_nodes {
            let handle = source_sender_node.handle().clone();
            let join_handle = start_source(
                source_sender_node,
                source_listener_node,
                start_barrier.clone(),
            )?;
            self.join_handles.insert(handle, join_handle);
        }
        Ok(())
    }

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
    start_barrier: Arc<Barrier>,
) -> Result<JoinHandle<()>, ExecutionError> {
    let handle = source_sender.handle().clone();
    let running = source_sender.running().clone();

    let _st_handle = Builder::new()
        .name(format!("{handle}-sender"))
        .spawn(move || {
            start_barrier.wait();
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
