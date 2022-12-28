#![allow(clippy::type_complexity)]

use crate::dag::channels::SourceChannelForwarder;
use crate::dag::dag::Dag;
use crate::dag::dag_metadata::{Consistency, DagMetadata, DagMetadataManager};
use crate::dag::dag_schemas::{DagSchemaManager, NodeSchemas};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{
    ChannelDisconnected, IncompatibleSchemas, InconsistentCheckpointMetadata, InternalError,
    InvalidNodeHandle,
};
use crate::dag::executor_utils::{
    build_receivers_lists, create_ports_databases, fill_ports_record_readers, index_edges,
    init_component, init_select, map_to_op,
};
use crate::dag::forwarder::{ProcessorChannelManager, SourceChannelManager, StateWriter};
use crate::dag::node::{NodeHandle, PortHandle, ProcessorFactory, SinkFactory, SourceFactory};
use crate::dag::record_store::RecordReader;
use crate::storage::common::Database;
use crate::storage::lmdb_storage::LmdbEnvironmentManager;

use crossbeam::channel::{bounded, Receiver, RecvTimeoutError, Sender};
use dozer_types::internal_err;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Record, Schema};

use crate::dag::epoch::{Epoch, EpochManager};
use log::info;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::thread::JoinHandle;
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
    Commit { epoch_details: Epoch },
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

pub(crate) struct StorageMetadata {
    pub env: LmdbEnvironmentManager,
    pub meta_db: Database,
}

impl StorageMetadata {
    pub fn new(env: LmdbEnvironmentManager, meta_db: Database) -> Self {
        Self { env, meta_db }
    }
}

struct InternalChannelSourceForwarder {
    sender: Sender<(PortHandle, u64, u64, Operation)>,
}

impl InternalChannelSourceForwarder {
    pub fn new(sender: Sender<(PortHandle, u64, u64, Operation)>) -> Self {
        Self { sender }
    }
}

impl SourceChannelForwarder for InternalChannelSourceForwarder {
    fn send(
        &mut self,
        txid: u64,
        seq_in_tx: u64,
        op: Operation,
        port: PortHandle,
    ) -> Result<(), ExecutionError> {
        internal_err!(self.sender.send((port, txid, seq_in_tx, op)))
    }
}

pub struct DagExecutor<'a> {
    dag: &'a Dag,
    schemas: HashMap<NodeHandle, NodeSchemas>,
    term_barrier: Arc<Barrier>,
    record_stores: Arc<RwLock<HashMap<NodeHandle, HashMap<PortHandle, RecordReader>>>>,
    join_handles: HashMap<NodeHandle, JoinHandle<()>>,
    path: PathBuf,
    options: ExecutorOptions,
    running: Arc<AtomicBool>,
    consistency_metadata: HashMap<NodeHandle, (u64, u64)>,
}

impl<'a> DagExecutor<'a> {
    fn check_consistency(
        dag: &'a Dag,
        path: &Path,
    ) -> Result<HashMap<NodeHandle, (u64, u64)>, ExecutionError> {
        let mut r: HashMap<NodeHandle, (u64, u64)> = HashMap::new();
        let meta = DagMetadataManager::new(dag, path)?;
        let chk = meta.get_checkpoint_consistency();
        for (handle, _factory) in &dag.get_sources() {
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
        dag: &'a Dag,
        path: &Path,
        options: ExecutorOptions,
        running: Arc<AtomicBool>,
    ) -> Result<Self, ExecutionError> {
        //

        let consistency_metadata: HashMap<NodeHandle, (u64, u64)> =
            match Self::check_consistency(dag, path) {
                Ok(c) => c,
                Err(_) => {
                    DagMetadataManager::new(dag, path)?.delete_metadata();
                    dag.get_sources()
                        .iter()
                        .map(|e| (e.0.clone(), (0_u64, 0_u64)))
                        .collect()
                }
            };

        let schemas = Self::load_or_init_schema(dag, path)?;

        Ok(Self {
            dag,
            schemas,
            term_barrier: Arc::new(Barrier::new(
                dag.get_sources().len() * 2 + dag.get_processors().len() + dag.get_sinks().len(),
            )),
            record_stores: Arc::new(RwLock::new(
                dag.nodes
                    .iter()
                    .map(|e| (e.0.clone(), HashMap::<PortHandle, RecordReader>::new()))
                    .collect(),
            )),
            path: path.to_path_buf(),
            join_handles: HashMap::new(),
            options,
            running,
            consistency_metadata,
        })
    }

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
                .get(port)
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
                .get(port)
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
                        .ok_or_else(|| InvalidNodeHandle(handle.clone()))?;
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

    fn start_source(
        &self,
        handle: NodeHandle,
        src_factory: Arc<dyn SourceFactory>,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        schemas: &NodeSchemas,
        epoch_manager: Arc<RwLock<EpochManager>>,
    ) -> Result<JoinHandle<()>, ExecutionError> {
        //
        //

        let st_handle = handle.clone();
        let (st_sender, st_receiver) =
            bounded::<(PortHandle, u64, u64, Operation)>(self.options.channel_buffer_sz);
        let st_src_factory = src_factory.clone();
        let st_running = self.running.clone();
        let st_output_schemas = schemas.output_schemas.clone();
        let st_term_barrier = self.term_barrier.clone();
        let mut fw = InternalChannelSourceForwarder::new(st_sender);
        let start_seq = *self
            .consistency_metadata
            .get(&handle)
            .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?;

        let _st_handle = thread::spawn(move || -> Result<(), ExecutionError> {
            let src = st_src_factory.build(st_output_schemas)?;
            let r = src.start(&mut fw, Some(start_seq));
            st_running.store(false, Ordering::SeqCst);
            info!("[{}-sender] Waiting on term barrier", st_handle);
            st_term_barrier.wait();
            r
        });

        let lt_handle = handle.clone();
        let lt_path = self.path.clone();
        let lt_output_ports = src_factory.get_output_ports();
        let lt_edges = self.dag.edges.clone();
        let lt_record_stores = self.record_stores.clone();
        let lt_executor_options = self.options.clone();
        let lt_running = self.running.clone();
        let lt_term_barrier = self.term_barrier.clone();
        let lt_output_schemas = schemas.output_schemas.clone();

        let lt_thread_fct = move || -> Result<(), ExecutionError> {
            let _output_schemas = HashMap::<PortHandle, Schema>::new();
            let mut state_meta = init_component(&handle, lt_path.as_path(), |_e| Ok(()))?;

            let port_databases = create_ports_databases(&mut state_meta.env, &lt_output_ports)?;

            let master_tx = state_meta.env.create_txn()?;

            fill_ports_record_readers(
                &handle,
                &lt_edges,
                &port_databases,
                &master_tx,
                &lt_record_stores,
                &lt_output_ports,
            );

            let mut dag_fw = SourceChannelManager::new(
                handle,
                senders,
                StateWriter::new(
                    state_meta.meta_db,
                    port_databases,
                    master_tx,
                    None,
                    lt_output_schemas,
                    HashMap::new(),
                ),
                true,
                epoch_manager,
            );
            loop {
                match st_receiver.recv_timeout(lt_executor_options.commit_time_threshold) {
                    Ok((port, txid, seq_in_tx, op)) => {
                        let stop_req = !lt_running.load(Ordering::SeqCst);
                        let committed = dag_fw.send_and_trigger_commit_if_needed(
                            txid, seq_in_tx, op, port, stop_req,
                        )?;
                        if committed && stop_req {
                            dag_fw.terminate()?;
                            info!("[{}-listener] Waiting on term barrier", lt_handle);
                            drop(st_receiver);
                            lt_term_barrier.wait();
                            return Ok(());
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        let stop_req = !lt_running.load(Ordering::SeqCst);
                        let committed = dag_fw.trigger_commit_if_needed(stop_req)?;
                        if committed && stop_req {
                            dag_fw.terminate()?;
                            info!("[{}-listener] Waiting on term barrier", lt_handle);
                            drop(st_receiver);
                            lt_term_barrier.wait();
                            return Ok(());
                        }
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        return Err(ChannelDisconnected);
                    }
                }
            }
        };

        Ok(thread::spawn(|| lt_thread_fct().unwrap()))
    }

    pub fn start_processor(
        &self,
        handle: NodeHandle,
        proc_factory: Arc<dyn ProcessorFactory>,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
        schemas: &NodeSchemas,
    ) -> Result<JoinHandle<()>, ExecutionError> {
        //
        let lt_handle = handle.clone();
        let lt_path = self.path.clone();
        let lt_output_ports = proc_factory.get_output_ports();
        let lt_edges = self.dag.edges.clone();
        let lt_record_stores = self.record_stores.clone();
        let lt_output_schemas = schemas.output_schemas.clone();
        let lt_input_schemas = schemas.input_schemas.clone();
        let lt_term_barrier = self.term_barrier.clone();

        let lt_thread_fct = move || -> Result<(), ExecutionError> {
            let mut proc =
                proc_factory.build(lt_input_schemas.clone(), lt_output_schemas.clone())?;
            let mut state_meta = init_component(&handle, lt_path.as_path(), |e| proc.init(e))?;

            let port_databases =
                create_ports_databases(&mut state_meta.env, &proc_factory.get_output_ports())?;

            let master_tx = state_meta.env.create_txn()?;

            fill_ports_record_readers(
                &handle,
                &lt_edges,
                &port_databases,
                &master_tx,
                &lt_record_stores,
                &lt_output_ports,
            );

            let (handles_ls, receivers_ls) = build_receivers_lists(receivers);
            let mut fw = ProcessorChannelManager::new(
                handle.clone(),
                senders,
                StateWriter::new(
                    state_meta.meta_db,
                    port_databases,
                    master_tx.clone(),
                    Some(proc_factory.get_input_ports()),
                    lt_output_schemas,
                    lt_input_schemas,
                ),
                true,
            );

            let mut port_states: Vec<InputPortState> =
                handles_ls.iter().map(|_h| InputPortState::Open).collect();

            let mut readable_ports = vec![true; receivers_ls.len()];
            let mut commits_received: usize = 0;
            let mut common_epoch = Epoch::new(0, HashMap::new());

            let mut sel = init_select(&receivers_ls);
            loop {
                let index = sel.ready();
                if !readable_ports[index] {
                    continue;
                }

                match internal_err!(receivers_ls[index].recv())? {
                    ExecutorOperation::Commit { epoch_details } => {
                        assert_eq!(epoch_details.id, common_epoch.id);
                        commits_received += 1;
                        readable_ports[index] = false;
                        common_epoch.details.extend(epoch_details.details);

                        if commits_received == receivers_ls.len() {
                            proc.commit(&common_epoch, &master_tx)?;
                            fw.store_and_send_commit(&common_epoch)?;
                            common_epoch = Epoch::new(common_epoch.id + 1, HashMap::new());
                            commits_received = 0;
                            for v in &mut readable_ports {
                                *v = true;
                            }
                        }
                    }
                    ExecutorOperation::Terminate => {
                        port_states[index] = InputPortState::Terminated;
                        info!(
                            "[{}] Received Terminate request on port {}",
                            handle, &handles_ls[index]
                        );
                        if port_states.iter().all(|v| v == &InputPortState::Terminated) {
                            fw.send_term_and_wait()?;
                            info!("[{}] Waiting on term barrier", lt_handle);
                            lt_term_barrier.wait();
                            return Ok(());
                        }
                    }
                    op => {
                        let guard = lt_record_stores.read();
                        let reader = guard
                            .get(&handle)
                            .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?;

                        let data_op = map_to_op(op)?;
                        proc.process(handles_ls[index], data_op, &mut fw, &master_tx, reader)?;
                    }
                }
            }
        };

        Ok(thread::spawn(|| lt_thread_fct().unwrap()))
    }

    pub fn start_sink(
        &self,
        handle: NodeHandle,
        snk_factory: Arc<dyn SinkFactory>,
        receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
        schemas: &NodeSchemas,
    ) -> Result<JoinHandle<()>, ExecutionError> {
        //

        let lt_handle = handle.clone();
        let lt_path = self.path.clone();
        let lt_record_stores = self.record_stores.clone();
        let lt_term_barrier = self.term_barrier.clone();
        let lt_input_schemas = schemas.input_schemas.clone();

        let lt_thread_fct = move || -> Result<(), ExecutionError> {
            let mut snk = snk_factory.build(lt_input_schemas.clone())?;
            let state_meta = init_component(&handle, lt_path.as_path(), |e| snk.init(e))?;

            let master_tx = state_meta.env.create_txn()?;

            let mut state_writer = StateWriter::new(
                state_meta.meta_db,
                HashMap::new(),
                master_tx.clone(),
                Some(snk_factory.get_input_ports()),
                HashMap::new(),
                lt_input_schemas,
            );

            let (handles_ls, receivers_ls) = build_receivers_lists(receivers);

            let mut port_states: Vec<InputPortState> =
                handles_ls.iter().map(|_h| InputPortState::Open).collect();

            let mut readable_ports: Vec<bool> = receivers_ls.iter().map(|_e| true).collect();
            let mut commits_received: usize = 0;
            let mut common_epoch = Epoch::new(0, HashMap::new());

            let mut sel = init_select(&receivers_ls);
            loop {
                let index = sel.ready();
                if !readable_ports[index] {
                    continue;
                }

                match internal_err!(receivers_ls[index].recv())? {
                    ExecutorOperation::Terminate => {
                        port_states[index] = InputPortState::Terminated;
                        info!(
                            "[{}] Received Terminate request on port {}",
                            handle, &handles_ls[index]
                        );
                        if port_states.iter().all(|v| v == &InputPortState::Terminated) {
                            info!("[{}] Waiting on term barrier", lt_handle);
                            lt_term_barrier.wait();
                            return Ok(());
                        }
                    }
                    ExecutorOperation::Commit { epoch_details } => {
                        assert_eq!(epoch_details.id, common_epoch.id);
                        commits_received += 1;
                        readable_ports[index] = false;
                        common_epoch.details.extend(epoch_details.details.clone());

                        if commits_received == receivers_ls.len() {
                            info!("[{}] Checkpointing - {}", handle, &epoch_details);
                            snk.commit(&epoch_details, &master_tx)?;
                            state_writer.store_commit_info(&epoch_details)?;
                            common_epoch = Epoch::new(common_epoch.id + 1, HashMap::new());
                            commits_received = 0;
                            for v in &mut readable_ports {
                                *v = true;
                            }
                        }
                    }
                    op => {
                        let data_op = map_to_op(op)?;
                        let guard = lt_record_stores.read();
                        let reader = guard
                            .get(&handle)
                            .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?;
                        snk.process(handles_ls[index], data_op, &master_tx, reader)?;
                    }
                }
            }
        };

        Ok(thread::spawn(|| lt_thread_fct().unwrap()))
    }

    pub fn start(&mut self) -> Result<(), ExecutionError> {
        let (mut senders, mut receivers) = index_edges(self.dag, self.options.channel_buffer_sz);

        for (handle, factory) in self.dag.get_sinks() {
            let join_handle = self
                .start_sink(
                    handle.clone(),
                    factory.clone(),
                    receivers
                        .remove(&handle)
                        .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?,
                    self.schemas
                        .get(&handle)
                        .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?,
                )
                .unwrap();
            self.join_handles.insert(handle.clone(), join_handle);
        }

        for (handle, factory) in self.dag.get_processors() {
            let join_handle = self
                .start_processor(
                    handle.clone(),
                    factory.clone(),
                    senders
                        .remove(&handle)
                        .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?,
                    receivers
                        .remove(&handle)
                        .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?,
                    self.schemas
                        .get(&handle)
                        .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?,
                )
                .unwrap();
            self.join_handles.insert(handle.clone(), join_handle);
        }

        let epoch_manager: Arc<RwLock<EpochManager>> = Arc::new(RwLock::new(EpochManager::new(
            self.options.commit_sz,
            self.options.commit_time_threshold,
            self.dag.get_sources().iter().map(|e| e.0.clone()).collect(),
        )));

        for (handle, factory) in self.dag.get_sources() {
            let join_handle = self
                .start_source(
                    handle.clone(),
                    factory.clone(),
                    senders
                        .remove(&handle)
                        .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?,
                    self.schemas
                        .get(&handle)
                        .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?,
                    epoch_manager.clone(),
                )
                .unwrap();
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
            let mut finished: usize = 0;
            for handle in &handles {
                if let Some(j) = self.join_handles.get(handle) {
                    if j.is_finished() {
                        let _r = self.join_handles.remove(handle).unwrap().join();
                        finished += 1;
                    }
                }
            }

            if finished == self.join_handles.len() {
                return Ok(());
            }

            thread::sleep(Duration::from_millis(250));
        }
    }
}
