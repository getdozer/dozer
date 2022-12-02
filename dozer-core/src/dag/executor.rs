use crate::dag::channels::SourceChannelForwarder;
use crate::dag::dag::{Dag, NodeType};
use crate::dag::dag_metadata::{DagMetadata, DagMetadataManager, METADATA_DB_NAME};
use crate::dag::dag_schemas::{DagSchemaManager, NodeSchemas};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{IncompatibleSchemas, InternalError, InvalidNodeHandle};
use crate::dag::node::{NodeHandle, PortHandle, SourceFactory};
use crate::dag::record_store::RecordReader;
use crate::storage::common::{Database, Environment, EnvironmentManager, RenewableRwTransaction};
use crate::storage::lmdb_storage::LmdbEnvironmentManager;
use crossbeam::channel::{bounded, Receiver, Sender};
use dozer_types::internal_err;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Record, Schema};
use fp_rust::sync::CountDownLatch;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct ExecutorOptions {
    pub commit_sz: u32,
    pub channel_buffer_sz: usize,
    pub commit_time_threshold: Duration,
}

impl ExecutorOptions {
    pub fn default() -> Self {
        Self {
            commit_sz: 10_000,
            channel_buffer_sz: 20_000,
            commit_time_threshold: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutorOperation {
    Delete { seq: u64, old: Record },
    Insert { seq: u64, new: Record },
    Update { seq: u64, old: Record, new: Record },
    Commit { source: NodeHandle, epoch: u64 },
    Terminate,
}

impl ExecutorOperation {
    pub fn from_operation(seq: u64, op: Operation) -> ExecutorOperation {
        match op {
            Operation::Update { old, new } => ExecutorOperation::Update { old, new, seq },
            Operation::Delete { old } => ExecutorOperation::Delete { old, seq },
            Operation::Insert { new } => ExecutorOperation::Insert { new, seq },
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

pub(crate) struct StorageMetadata {
    pub env: Box<dyn EnvironmentManager>,
    pub meta_db: Database,
}

impl StorageMetadata {
    pub fn new(env: Box<dyn EnvironmentManager>, meta_db: Database) -> Self {
        Self { env, meta_db }
    }
}

struct InternalChannelSourceForwarder {
    senders: HashMap<PortHandle, Sender<ExecutorOperation>>,
}

impl InternalChannelSourceForwarder {
    pub fn new(senders: HashMap<PortHandle, Sender<ExecutorOperation>>) -> Self {
        Self { senders }
    }
}

impl SourceChannelForwarder for InternalChannelSourceForwarder {
    fn send(&mut self, seq: u64, op: Operation, port: PortHandle) -> Result<(), ExecutionError> {
        let sender = self
            .senders
            .get(&port)
            .ok_or(ExecutionError::InvalidPortHandle(port))?;
        let exec_op = ExecutorOperation::from_operation(seq, op);
        internal_err!(sender.send(exec_op))
    }

    fn terminate(&mut self) -> Result<(), ExecutionError> {
        for sender in &self.senders {
            let _ = sender.1.send(ExecutorOperation::Terminate);
        }
        Ok(())
    }
}

pub struct DagExecutor<'a> {
    dag: &'a Dag,
    schemas: HashMap<NodeHandle, NodeSchemas>,
    term_barrier: Arc<Barrier>,
    start_latch: Arc<CountDownLatch>,
    record_stores: Arc<RwLock<HashMap<NodeHandle, HashMap<PortHandle, RecordReader>>>>,
    join_handles: HashMap<NodeHandle, JoinHandle<Result<(), ExecutionError>>>,
    path: PathBuf,
    options: ExecutorOptions,
}

impl<'a> DagExecutor<'a> {
    fn validate_schemas(
        current: &NodeSchemas,
        existing: &DagMetadata,
    ) -> Result<(), ExecutionError> {
        if existing.output_schemas.len() != current.output_schemas.len() {
            return Err(IncompatibleSchemas());
        }
        for (port, schema) in &current.output_schemas {
            let other_schema = existing
                .output_schemas
                .get(&port)
                .ok_or(IncompatibleSchemas())?;
            if schema != other_schema {
                return Err(IncompatibleSchemas());
            }
        }
        if existing.input_schemas.len() != current.input_schemas.len() {
            return Err(IncompatibleSchemas());
        }
        for (port, schema) in &current.output_schemas {
            let other_schema = existing
                .output_schemas
                .get(&port)
                .ok_or(IncompatibleSchemas())?;
            if schema != other_schema {
                return Err(IncompatibleSchemas());
            }
        }
        Ok(())
    }

    fn load_or_init_schema(
        dag: &'a Dag,
        path: &Path,
    ) -> Result<HashMap<NodeHandle, NodeSchemas>, ExecutionError> {
        let schema_manager = DagSchemaManager::new(dag)?;
        let meta_manager = DagMetadataManager::new(dag, path)?;

        let compatible = match meta_manager.get_metadata() {
            Ok(existing_schemas) => {
                for (handle, current) in schema_manager.get_all_schemas() {
                    let existing = existing_schemas
                        .get(handle)
                        .ok_or(InvalidNodeHandle(handle.clone()))?;
                    Self::validate_schemas(current, existing)?;
                }
                Ok(schema_manager.get_all_schemas().clone())
            }
            Err(_) => Err(IncompatibleSchemas()),
        };

        match compatible {
            Ok(schema) => Ok(schema),
            Err(_) => {
                meta_manager.delete_metadata();
                meta_manager.init_metadata(schema_manager.get_all_schemas())?;
                Ok(schema_manager.get_all_schemas().clone())
            }
        }
    }

    pub fn new(
        dag: &'a Dag,
        path: &Path,
        options: ExecutorOptions,
    ) -> Result<Self, ExecutionError> {
        //
        let schemas = Self::load_or_init_schema(dag, path)?;
        Ok(Self {
            dag,
            schemas,
            term_barrier: Arc::new(Barrier::new(0)),
            start_latch: Arc::new(CountDownLatch::new(0)),
            record_stores: Arc::new(RwLock::new(
                dag.nodes
                    .iter()
                    .map(|e| (e.0.clone(), HashMap::<PortHandle, RecordReader>::new()))
                    .collect(),
            )),
            path: path.to_path_buf(),
            join_handles: HashMap::new(),
            options: options,
        })
    }

    fn create_channels(
        &self,
    ) -> (
        HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>>,
        HashMap<NodeHandle, HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>>,
    ) {
        let mut senders: HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>> =
            HashMap::new();
        let mut receivers: HashMap<
            NodeHandle,
            HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
        > = HashMap::new();

        for edge in self.dag.edges.iter() {
            if !senders.contains_key(&edge.from.node) {
                senders.insert(edge.from.node.clone(), HashMap::new());
            }
            if !receivers.contains_key(&edge.to.node) {
                receivers.insert(edge.to.node.clone(), HashMap::new());
            }

            let (tx, rx) = bounded(self.options.channel_buffer_sz);

            let rcv_port: PortHandle = edge.to.port;
            if receivers
                .get(&edge.to.node)
                .unwrap()
                .contains_key(&rcv_port)
            {
                receivers
                    .get_mut(&edge.to.node)
                    .unwrap()
                    .get_mut(&rcv_port)
                    .unwrap()
                    .push(rx);
            } else {
                receivers
                    .get_mut(&edge.to.node)
                    .unwrap()
                    .insert(rcv_port, vec![rx]);
            }

            let snd_port: PortHandle = edge.from.port;
            if senders
                .get(&edge.from.node)
                .unwrap()
                .contains_key(&snd_port)
            {
                senders
                    .get_mut(&edge.from.node)
                    .unwrap()
                    .get_mut(&snd_port)
                    .unwrap()
                    .push(tx);
            } else {
                senders
                    .get_mut(&edge.from.node)
                    .unwrap()
                    .insert(snd_port, vec![tx]);
            }
        }

        (senders, receivers)
    }

    fn init_node_storage<F>(
        &self,
        node_handle: &NodeHandle,
        mut init_f: F,
    ) -> Result<StorageMetadata, ExecutionError>
    where
        F: FnMut(&mut dyn Environment) -> Result<(), ExecutionError>,
    {
        let mut env = LmdbEnvironmentManager::create(self.path.as_path(), node_handle.as_str())?;
        let db = env.open_database(METADATA_DB_NAME, false)?;
        init_f(env.as_environment())?;
        Ok(StorageMetadata::new(env, db))
    }

    fn start_source(
        &self,
        handle: NodeHandle,
        src_factory: Arc<dyn SourceFactory>,
    ) -> Result<JoinHandle<Result<(), ExecutionError>>, ExecutionError> {
        let mut internal_receivers: Vec<Receiver<ExecutorOperation>> = Vec::new();
        let mut internal_senders: HashMap<PortHandle, Sender<ExecutorOperation>> = HashMap::new();

        for port in &src_factory.get_output_ports() {
            let channels = bounded::<ExecutorOperation>(self.options.channel_buffer_sz);
            internal_receivers.push(channels.1);
            internal_senders.insert(port.handle, channels.0);
        }

        let mut fw = InternalChannelSourceForwarder::new(internal_senders);
        thread::spawn(move || -> Result<(), ExecutionError> {
            let src = src_factory.build();
            src.start(&mut fw, None)
        });

        let listener_handle = handle.clone();
        Ok(thread::spawn(move || -> Result<(), ExecutionError> {
            //  let _output_schemas = HashMap::<PortHandle, Schema>::new();
            //  let mut state_meta = self.init_node_storage(&handle, |_e| Ok(()))?;
            //
            //         let port_databases =
            //             create_ports_databases(state_meta.env.as_environment(), &output_ports)?;
            //
            //         let master_tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
            //             Arc::new(RwLock::new(state_meta.env.create_txn()?));
            //

            Ok(())
        }))
    }

    pub fn start(&self) -> Result<(), ExecutionError> {
        let (mut senders, mut receivers) = self.create_channels();

        for source in self.dag.get_sources() {}
        Ok(())
    }
}
