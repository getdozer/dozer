#![allow(clippy::type_complexity)]

use crate::dag::dag_metadata::{Consistency, DagMetadata, DagMetadataManager};
use crate::dag::dag_schemas::{DagSchemas, NodeSchemas};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{IncompatibleSchemas, InconsistentCheckpointMetadata};
use crate::dag::executor_utils::index_edges;
use crate::dag::node::{NodeHandle, PortHandle, ProcessorFactory, SinkFactory, SourceFactory};
use crate::dag::record_store::RecordReader;
use crate::dag::Dag;

use crossbeam::channel::{bounded, Receiver, Sender};
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Record, Schema};

use crate::dag::epoch::{Epoch, EpochManager};
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

mod name;
mod node;
mod processor_node;
mod receiver_loop;
mod sink_node;
mod source_node;

use node::Node;
use processor_node::ProcessorNode;
use sink_node::SinkNode;

use self::source_node::{SourceListenerNode, SourceSenderNode};

use super::epoch::OpIdentifier;

pub struct DagExecutor<'a, T: Clone> {
    dag: &'a Dag<T>,
    schemas: HashMap<NodeHandle, NodeSchemas<T>>,
    record_stores: Arc<RwLock<HashMap<NodeHandle, HashMap<PortHandle, Box<dyn RecordReader>>>>>,
    join_handles: HashMap<NodeHandle, JoinHandle<()>>,
    path: PathBuf,
    options: ExecutorOptions,
    running: Arc<AtomicBool>,
    consistency_metadata: HashMap<NodeHandle, Option<OpIdentifier>>,
}

impl<'a, T: Clone + Debug + 'static> DagExecutor<'a, T> {
    fn check_consistency(
        dag: &'a Dag<T>,
        path: &Path,
    ) -> Result<HashMap<NodeHandle, Option<OpIdentifier>>, ExecutionError> {
        let mut r = HashMap::new();
        let meta = DagMetadataManager::new(dag, path)?;
        let chk = meta.get_checkpoint_consistency();
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
        dag: &'a Dag<T>,
        path: &Path,
        options: ExecutorOptions,
        running: Arc<AtomicBool>,
    ) -> Result<Self, ExecutionError> {
        //

        let consistency_metadata = match Self::check_consistency(dag, path) {
            Ok(c) => c,
            Err(_) => {
                DagMetadataManager::new(dag, path)?.delete_metadata();
                dag.sources()
                    .map(|(handle, _)| (handle.clone(), None))
                    .collect()
            }
        };

        let schemas = Self::load_or_init_schema(dag, path)?;

        Ok(Self {
            dag,
            schemas,
            record_stores: Arc::new(RwLock::new(
                dag.node_handles()
                    .map(|node_handle| {
                        (
                            node_handle.clone(),
                            HashMap::<PortHandle, Box<dyn RecordReader>>::new(),
                        )
                    })
                    .collect(),
            )),
            path: path.to_path_buf(),
            join_handles: HashMap::new(),
            options,
            running,
            consistency_metadata,
        })
    }

    pub fn validate(dag: &'a Dag<T>, path: &Path) -> Result<(), ExecutionError> {
        Self::load_or_init_schema(dag, path).map(|_| ())
    }

    fn validate_schemas(
        current: &NodeSchemas<T>,
        existing: &DagMetadata,
    ) -> Result<(), ExecutionError> {
        if existing.output_schemas.len() != current.output_schemas.len() {
            return Err(IncompatibleSchemas());
        }
        for (port, (schema, _ctx)) in &current.output_schemas {
            let other_schema = existing
                .output_schemas
                .get(port)
                .ok_or(IncompatibleSchemas())?;
            if schema != other_schema {
                return Err(IncompatibleSchemas());
            }
        }
        if existing.input_schemas.len() != current.input_schemas.len() {
            return Err(IncompatibleSchemas());
        }
        for (port, (schema, _ctx)) in &current.output_schemas {
            let other_schema = existing
                .output_schemas
                .get(port)
                .ok_or(IncompatibleSchemas())?;
            if schema != other_schema {
                return Err(IncompatibleSchemas());
            }
        }
        Ok(())
    }

    fn load_or_init_schema(
        dag: &'a Dag<T>,
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

    fn start_source(
        &self,
        handle: NodeHandle,
        src_factory: Arc<dyn SourceFactory<T>>,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        schemas: &NodeSchemas<T>,
        epoch_manager: Arc<EpochManager>,
        start_barrier: Arc<Barrier>,
    ) -> Result<JoinHandle<()>, ExecutionError> {
        let (sender, receiver) = bounded(self.options.channel_buffer_sz);
        // let (sender, receiver) = bounded(1);

        let start_seq = *self
            .consistency_metadata
            .get(&handle)
            .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?;
        let output_ports = src_factory.get_output_ports()?;

        let st_node_handle = handle.clone();
        let output_schemas: HashMap<PortHandle, Schema> = schemas
            .output_schemas
            .clone()
            .into_iter()
            .map(|e| (e.0, e.1 .0))
            .collect();
        let running = self.running.clone();
        let running_source = running.clone();
        let source_fn = move |handle: NodeHandle| -> Result<(), ExecutionError> {
            let sender = SourceSenderNode::new(
                handle,
                &*src_factory,
                output_schemas,
                start_seq,
                sender,
                running,
            )?;
            sender.run()
        };

        let _st_handle = Builder::new()
            .name(format!("{handle}-sender"))
            .spawn(move || {
                if let Err(e) = source_fn(st_node_handle) {
                    if running_source.load(Ordering::Relaxed) {
                        std::panic::panic_any(e);
                    }
                }
            })?;

        let timeout = self.options.commit_time_threshold;
        let base_path = self.path.clone();
        let record_readers = self.record_stores.clone();
        let edges = self.dag.edge_handles().cloned().collect::<Vec<_>>();
        let running = self.running.clone();
        let running_listener = running.clone();
        let commit_sz = self.options.commit_sz;
        let max_duration_between_commits = self.options.commit_time_threshold;
        let output_schemas: HashMap<PortHandle, Schema> = schemas
            .output_schemas
            .clone()
            .into_iter()
            .map(|e| (e.0, e.1 .0))
            .collect();
        let retention_queue_size = self.options.channel_buffer_sz + 1;
        let source_fn = move |handle: NodeHandle| -> Result<(), ExecutionError> {
            let listener = SourceListenerNode::new(
                handle,
                receiver,
                timeout,
                &base_path,
                &output_ports,
                record_readers,
                senders,
                &edges,
                running,
                commit_sz,
                max_duration_between_commits,
                epoch_manager,
                output_schemas,
                retention_queue_size,
            )?;
            start_barrier.wait();
            listener.run()
        };
        Ok(Builder::new()
            .name(format!("{handle}-listener"))
            .spawn(move || {
                if let Err(e) = source_fn(handle) {
                    if running_listener.load(Ordering::Relaxed) {
                        std::panic::panic_any(e);
                    }
                }
            })?)
    }

    pub fn start_processor(
        &self,
        handle: NodeHandle,
        proc_factory: Arc<dyn ProcessorFactory<T>>,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
        schemas: &NodeSchemas<T>,
    ) -> Result<JoinHandle<()>, ExecutionError> {
        let base_path = self.path.clone();
        let record_readers = self.record_stores.clone();
        let edges = self.dag.edge_handles().cloned().collect::<Vec<_>>();
        let input_schemas: HashMap<PortHandle, Schema> = schemas
            .input_schemas
            .clone()
            .into_iter()
            .map(|e| (e.0, e.1 .0))
            .collect();
        let output_schemas: HashMap<PortHandle, Schema> = schemas
            .output_schemas
            .clone()
            .into_iter()
            .map(|e| (e.0, e.1 .0))
            .collect();
        let running = self.running.clone();
        let retention_queue_size = self.options.channel_buffer_sz + 1;
        let processor_fn = move |handle: NodeHandle| -> Result<(), ExecutionError> {
            let processor = ProcessorNode::new(
                handle,
                &*proc_factory,
                &base_path,
                record_readers,
                receivers,
                senders,
                &edges,
                input_schemas,
                output_schemas,
                retention_queue_size,
            )?;
            processor.run()
        };
        Ok(Builder::new().name(handle.to_string()).spawn(move || {
            if let Err(e) = processor_fn(handle) {
                if running.load(Ordering::Relaxed) {
                    std::panic::panic_any(e);
                }
            }
        })?)
    }

    pub fn start_sink(
        &self,
        handle: NodeHandle,
        snk_factory: Arc<dyn SinkFactory<T>>,
        receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
        schemas: &NodeSchemas<T>,
    ) -> Result<JoinHandle<()>, ExecutionError> {
        let base_path = self.path.clone();
        let record_readers = self.record_stores.clone();
        let input_schemas: HashMap<PortHandle, Schema> = schemas
            .input_schemas
            .clone()
            .into_iter()
            .map(|e| (e.0, e.1 .0))
            .collect();
        let retention_queue_size = self.options.channel_buffer_sz + 1;
        let snk_fn = move |handle| -> Result<(), ExecutionError> {
            let sink = SinkNode::new(
                handle,
                &*snk_factory,
                &base_path,
                record_readers,
                receivers,
                input_schemas,
                retention_queue_size,
            )?;
            sink.run()
        };
        Ok(Builder::new().name(handle.to_string()).spawn(|| {
            if let Err(e) = snk_fn(handle) {
                std::panic::panic_any(e);
            }
        })?)
    }

    pub fn start(&mut self) -> Result<(), ExecutionError> {
        let (mut senders, mut receivers) = index_edges(self.dag, self.options.channel_buffer_sz);

        for (handle, factory) in self.dag.sinks() {
            let join_handle = self.start_sink(
                handle.clone(),
                factory.clone(),
                receivers.remove(handle).unwrap_or_default(),
                self.schemas
                    .get(handle)
                    .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?,
            )?;
            self.join_handles.insert(handle.clone(), join_handle);
        }

        for (handle, factory) in self.dag.processors() {
            let join_handle = self.start_processor(
                handle.clone(),
                factory.clone(),
                senders.remove(handle).unwrap_or_default(),
                receivers.remove(handle).unwrap_or_default(),
                self.schemas
                    .get(handle)
                    .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?,
            )?;
            self.join_handles.insert(handle.clone(), join_handle);
        }

        let num_sources = self.dag.sources().count();
        let epoch_manager: Arc<EpochManager> = Arc::new(EpochManager::new(num_sources));

        let start_barrier = Arc::new(Barrier::new(num_sources));

        for (handle, factory) in self.dag.sources() {
            let join_handle = self.start_source(
                handle.clone(),
                factory.clone(),
                senders.remove(handle).unwrap_or_default(),
                self.schemas
                    .get(handle)
                    .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?,
                epoch_manager.clone(),
                start_barrier.clone(),
            )?;
            self.join_handles.insert(handle.clone(), join_handle);
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
