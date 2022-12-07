#![allow(clippy::type_complexity)]

use crate::dag::channels::SourceChannelForwarder;
use crate::dag::dag::Dag;
use crate::dag::dag_metadata::{DagMetadata, DagMetadataManager};
use crate::dag::dag_schemas::{DagSchemaManager, NodeSchemas};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{
    ChannelDisconnected, IncompatibleSchemas, InternalError, InternalThreadPanic, InvalidNodeHandle,
};
use crate::dag::executor_utils::{
    build_receivers_lists, create_ports_databases, fill_ports_record_readers, index_edges,
    init_component, init_select, map_to_op,
};
use crate::dag::forwarder::{LocalChannelForwarder, StateWriter};
use crate::dag::node::{NodeHandle, PortHandle, ProcessorFactory, SinkFactory, SourceFactory};
use crate::dag::record_store::RecordReader;
use crate::storage::common::{Database, EnvironmentManager, RenewableRwTransaction};

use crate::storage::transactions::SharedTransaction;
use crossbeam::channel::{bounded, Receiver, RecvTimeoutError, Sender};
use dozer_types::internal_err;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Record, Schema};
use fp_rust::sync::CountDownLatch;
use log::{debug, info};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::ops::Add;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

macro_rules! term_on_err {
    ($e: expr, $stop_req: expr, $barrier: expr) => {
        if let Err(e) = $e {
            $stop_req.store(STOP_REQ_SHUTDOWN, Ordering::Relaxed);
            $barrier.wait();
            return Err(e);
        }
    };
}

const STOP_REQ_SHUTDOWN: u8 = 0xff_u8;
const STOP_REQ_TERM_AND_SHUTDOWN: u8 = 0xfe_u8;
const STOP_REQ_NO_ACTION: u8 = 0x00_u8;

#[derive(Clone)]
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
pub(crate) enum InputPortState {
    Open,
    Terminated,
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
    sender: Sender<(PortHandle, u64, Operation)>,
}

impl InternalChannelSourceForwarder {
    pub fn new(sender: Sender<(PortHandle, u64, Operation)>) -> Self {
        Self { sender }
    }
}

impl SourceChannelForwarder for InternalChannelSourceForwarder {
    fn send(&mut self, seq: u64, op: Operation, port: PortHandle) -> Result<(), ExecutionError> {
        internal_err!(self.sender.send((port, seq, op)))
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
    stop_req: Arc<AtomicU8>,
}

impl<'a> DagExecutor<'a> {
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
            term_barrier: Arc::new(Barrier::new(dag.get_sources().len())),
            start_latch: Arc::new(CountDownLatch::new(
                dag.get_sinks().len() as u64 + dag.get_processors().len() as u64,
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
            stop_req: Arc::new(AtomicU8::new(STOP_REQ_NO_ACTION)),
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
    ) -> Result<JoinHandle<Result<(), ExecutionError>>, ExecutionError> {
        //
        //

        let (st_sender, st_receiver) =
            bounded::<(PortHandle, u64, Operation)>(self.options.channel_buffer_sz);
        let st_src_factory = src_factory.clone();
        let st_stop_req = self.stop_req.clone();
        let st_output_schemas = schemas.output_schemas.clone();
        let mut fw = InternalChannelSourceForwarder::new(st_sender);

        let _st_handle = thread::spawn(move || -> Result<(), ExecutionError> {
            let src = st_src_factory.build(st_output_schemas);
            let r = src.start(&mut fw, None);
            st_stop_req.store(STOP_REQ_TERM_AND_SHUTDOWN, Ordering::Relaxed);
            r
        });

        let lt_handle = handle.clone();
        let lt_path = self.path.clone();
        let lt_output_ports = src_factory.get_output_ports();
        let lt_edges = self.dag.edges.clone();
        let lt_record_stores = self.record_stores.clone();
        let lt_executor_options = self.options.clone();
        let lt_stop_req = self.stop_req.clone();
        let lt_term_barrier = self.term_barrier.clone();
        let lt_output_schemas = schemas.output_schemas.clone();

        Ok(thread::spawn(move || -> Result<(), ExecutionError> {
            let _output_schemas = HashMap::<PortHandle, Schema>::new();
            let mut state_meta = init_component(&handle, lt_path.as_path(), |_e| Ok(()))?;

            let port_databases =
                create_ports_databases(state_meta.env.as_environment(), &lt_output_ports)?;

            let master_tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
                Arc::new(RwLock::new(state_meta.env.create_txn()?));

            fill_ports_record_readers(
                &handle,
                &lt_edges,
                &port_databases,
                &master_tx,
                &lt_record_stores,
                &lt_output_ports,
            );

            let mut dag_fw = LocalChannelForwarder::new_source_forwarder(
                handle,
                senders,
                lt_executor_options.commit_sz,
                lt_executor_options.commit_time_threshold,
                StateWriter::new(
                    state_meta.meta_db,
                    port_databases,
                    master_tx.clone(),
                    None,
                    lt_output_schemas,
                    HashMap::new(),
                ),
                true,
            );
            loop {
                let r = st_receiver.recv_deadline(Instant::now().add(Duration::from_millis(500)));
                match lt_stop_req.load(Ordering::Relaxed) {
                    STOP_REQ_TERM_AND_SHUTDOWN => {
                        term_on_err!(dag_fw.commit_and_terminate(), lt_stop_req, lt_term_barrier);
                        lt_term_barrier.wait();
                        break;
                    }
                    STOP_REQ_SHUTDOWN => {
                        lt_term_barrier.wait();
                        return Ok(());
                    }
                    _ => match r {
                        Err(RecvTimeoutError::Timeout) => {
                            term_on_err!(
                                dag_fw.trigger_commit_if_needed(),
                                lt_stop_req,
                                lt_term_barrier
                            )
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            lt_stop_req.store(STOP_REQ_SHUTDOWN, Ordering::Relaxed);
                            lt_term_barrier.wait();
                            return Err(ChannelDisconnected);
                        }
                        Ok((port, seq, Operation::Insert { new })) => {
                            term_on_err!(
                                dag_fw.send(seq, Operation::Insert { new }, port),
                                lt_stop_req,
                                lt_term_barrier
                            )
                        }
                        Ok((port, seq, Operation::Delete { old })) => {
                            term_on_err!(
                                dag_fw.send(seq, Operation::Delete { old }, port),
                                lt_stop_req,
                                lt_term_barrier
                            )
                        }
                        Ok((port, seq, Operation::Update { old, new })) => {
                            term_on_err!(
                                dag_fw.send(seq, Operation::Update { old, new }, port),
                                lt_stop_req,
                                lt_term_barrier
                            )
                        }
                    },
                }
            }
            Ok(())
        }))
    }

    fn close_senders(senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>) {
        for (_port, port_senders) in senders {
            for sender in port_senders {
                drop(sender);
            }
        }
    }

    pub fn start_processor(
        &self,
        handle: NodeHandle,
        proc_factory: Arc<dyn ProcessorFactory>,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
        schemas: &NodeSchemas,
    ) -> Result<JoinHandle<Result<(), ExecutionError>>, ExecutionError> {
        //
        let lt_path = self.path.clone();
        let lt_output_ports = proc_factory.get_output_ports();
        let lt_edges = self.dag.edges.clone();
        let lt_record_stores = self.record_stores.clone();
        let lt_start_latch = self.start_latch.clone();
        let lt_stop_req = self.stop_req.clone();
        let lt_term_barrier = self.term_barrier.clone();
        let lt_output_schemas = schemas.output_schemas.clone();
        let lt_input_schemas = schemas.input_schemas.clone();

        Ok(thread::spawn(move || -> Result<(), ExecutionError> {
            let mut proc = proc_factory.build(lt_input_schemas.clone(), lt_output_schemas.clone());
            let mut state_meta = init_component(&handle, lt_path.as_path(), |e| proc.init(e))?;

            let port_databases = create_ports_databases(
                state_meta.env.as_environment(),
                &proc_factory.get_output_ports(),
            )?;

            let master_tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
                Arc::new(RwLock::new(state_meta.env.create_txn()?));

            fill_ports_record_readers(
                &handle,
                &lt_edges,
                &port_databases,
                &master_tx,
                &lt_record_stores,
                &lt_output_ports,
            );

            let (handles_ls, receivers_ls) = build_receivers_lists(receivers);
            let mut fw = LocalChannelForwarder::new_processor_forwarder(
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

            lt_start_latch.countdown();

            let mut port_states: Vec<InputPortState> =
                handles_ls.iter().map(|_h| InputPortState::Open).collect();

            let mut sel = init_select(&receivers_ls);
            loop {
                let ready = sel.ready_timeout(Duration::from_millis(50));
                match ready {
                    Err(_e) => {
                        if lt_stop_req.load(Ordering::Relaxed) == STOP_REQ_SHUTDOWN {
                            debug!("[{}] STOP_REQ_SHUTDOWN", handle);
                            return Ok(());
                        }
                    }
                    Ok(index) => {
                        let r = receivers_ls[index].recv();
                        match lt_stop_req.load(Ordering::Relaxed) {
                            STOP_REQ_SHUTDOWN => {
                                return Ok(());
                            }
                            _ => match r {
                                Err(_) => {
                                    lt_stop_req.store(STOP_REQ_SHUTDOWN, Ordering::Relaxed);
                                    return Err(ChannelDisconnected);
                                }
                                Ok(ExecutorOperation::Commit { epoch, source }) => {
                                    if let Err(e) =
                                        proc.commit(&mut SharedTransaction::new(&master_tx))
                                    {
                                        Self::shutdown_and_wait(
                                            &lt_stop_req,
                                            &lt_term_barrier,
                                            receivers_ls,
                                        );
                                        return Err(e);
                                    }

                                    if let Err(e) = fw.store_and_send_commit(source, epoch) {
                                        Self::shutdown_and_wait(
                                            &lt_stop_req,
                                            &lt_term_barrier,
                                            receivers_ls,
                                        );
                                        return Err(e);
                                    }
                                }
                                Ok(ExecutorOperation::Terminate) => {
                                    port_states[index] = InputPortState::Terminated;
                                    info!(
                                        "[{}] Received Terminate request on port {}",
                                        handle, &handles_ls[index]
                                    );
                                    if port_states.iter().all(|v| v == &InputPortState::Terminated)
                                    {
                                        match fw.send_term_and_wait() {
                                            Err(e) => {
                                                Self::shutdown_and_wait(
                                                    &lt_stop_req,
                                                    &lt_term_barrier,
                                                    receivers_ls,
                                                );
                                                return Err(e);
                                            }
                                            Ok(_) => {
                                                return Ok(());
                                            }
                                        }
                                    }
                                }
                                Ok(op) => {
                                    let guard = lt_record_stores.read();
                                    let reader = guard.get(&handle).ok_or_else(|| {
                                        ExecutionError::InvalidNodeHandle(handle.clone())
                                    })?;

                                    let data_op = map_to_op(op)?;
                                    fw.update_seq_no(data_op.0);

                                    if let Err(e) = proc.process(
                                        handles_ls[index],
                                        data_op.1,
                                        &mut fw,
                                        &mut SharedTransaction::new(&master_tx),
                                        reader,
                                    ) {
                                        Self::shutdown_and_wait(
                                            &lt_stop_req,
                                            &lt_term_barrier,
                                            receivers_ls,
                                        );
                                        return Err(e);
                                    }
                                }
                            },
                        }
                    }
                }
            }
        }))
    }

    fn shutdown_and_wait(
        stop_req: &Arc<AtomicU8>,
        _stop_barrier: &Arc<Barrier>,
        receivers: Vec<Receiver<ExecutorOperation>>,
    ) {
        for receiver in receivers {
            drop(receiver);
        }
        stop_req.store(STOP_REQ_SHUTDOWN, Ordering::Relaxed);
    }

    pub fn start_sink(
        &self,
        handle: NodeHandle,
        snk_factory: Arc<dyn SinkFactory>,
        receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
        schemas: &NodeSchemas,
    ) -> Result<JoinHandle<Result<(), ExecutionError>>, ExecutionError> {
        //

        let lt_path = self.path.clone();
        let lt_record_stores = self.record_stores.clone();
        let lt_start_latch = self.start_latch.clone();
        let lt_stop_req = self.stop_req.clone();
        let lt_term_barrier = self.term_barrier.clone();
        let lt_input_schemas = schemas.input_schemas.clone();

        Ok(thread::spawn(move || -> Result<(), ExecutionError> {
            let mut snk = snk_factory.build(lt_input_schemas.clone());
            let mut state_meta = init_component(&handle, lt_path.as_path(), |e| snk.init(e))?;

            let master_tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
                Arc::new(RwLock::new(state_meta.env.create_txn()?));

            let mut state_writer = StateWriter::new(
                state_meta.meta_db,
                HashMap::new(),
                master_tx.clone(),
                Some(snk_factory.get_input_ports()),
                HashMap::new(),
                lt_input_schemas,
            );

            let (handles_ls, receivers_ls) = build_receivers_lists(receivers);
            lt_start_latch.countdown();

            let mut sel = init_select(&receivers_ls);
            loop {
                let ready = sel.ready_deadline(Instant::now().add(Duration::from_millis(50)));
                match ready {
                    Err(_e) => {
                        if lt_stop_req.load(Ordering::Relaxed) == STOP_REQ_SHUTDOWN {
                            debug!("[{}] STOP_REQ_SHUTDOWN", handle);
                            return Ok(());
                        }
                    }
                    Ok(index) => {
                        let r = receivers_ls[index].recv();
                        match lt_stop_req.load(Ordering::Relaxed) {
                            STOP_REQ_SHUTDOWN => {
                                debug!("[{}] STOP_REQ_SHUTDOWN", handle);
                                return Ok(());
                            }
                            _ => match r {
                                Err(_e) => {
                                    debug!("[{}] RecvError", handle);
                                    lt_stop_req.store(STOP_REQ_SHUTDOWN, Ordering::Relaxed);
                                    return Err(ChannelDisconnected);
                                }
                                Ok(ExecutorOperation::Terminate) => {
                                    info!("[{}] Terminating: Exiting message loop", handle);
                                    return Ok(());
                                }
                                Ok(ExecutorOperation::Commit { epoch, source }) => {
                                    info!(
                                        "[{}] Checkpointing (source: {}, epoch: {})",
                                        handle, source, epoch
                                    );

                                    if let Err(e) =
                                        snk.commit(&mut SharedTransaction::new(&master_tx))
                                    {
                                        Self::shutdown_and_wait(
                                            &lt_stop_req,
                                            &lt_term_barrier,
                                            receivers_ls,
                                        );
                                        return Err(e);
                                    }

                                    if let Err(e) = state_writer.store_commit_info(&source, epoch) {
                                        Self::shutdown_and_wait(
                                            &lt_stop_req,
                                            &lt_term_barrier,
                                            receivers_ls,
                                        );
                                        return Err(e);
                                    }
                                }
                                Ok(op) => {
                                    let data_op = map_to_op(op)?;

                                    let guard = lt_record_stores.read();
                                    let reader = guard.get(&handle).ok_or_else(|| {
                                        ExecutionError::InvalidNodeHandle(handle.clone())
                                    })?;

                                    if let Err(e) = snk.process(
                                        handles_ls[index],
                                        data_op.0,
                                        data_op.1,
                                        &mut SharedTransaction::new(&master_tx),
                                        reader,
                                    ) {
                                        Self::shutdown_and_wait(
                                            &lt_stop_req,
                                            &lt_term_barrier,
                                            receivers_ls,
                                        );
                                        return Err(e);
                                    }
                                }
                            },
                        }
                    }
                }
            }
        }))
    }

    pub fn start(&mut self) -> Result<(), ExecutionError> {
        let (mut senders, mut receivers) = index_edges(self.dag, self.options.channel_buffer_sz);

        for (handle, factory) in self.dag.get_sinks() {
            let join_handle = self.start_sink(
                handle.clone(),
                factory.clone(),
                receivers
                    .remove(&handle)
                    .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?,
                self.schemas
                    .get(&handle)
                    .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?,
            )?;
            self.join_handles.insert(handle.clone(), join_handle);
        }

        for (handle, factory) in self.dag.get_processors() {
            let join_handle = self.start_processor(
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
            )?;
            self.join_handles.insert(handle.clone(), join_handle);
        }

        self.start_latch.wait();

        for (handle, factory) in self.dag.get_sources() {
            let join_handle = self.start_source(
                handle.clone(),
                factory.clone(),
                senders
                    .remove(&handle)
                    .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?,
                self.schemas
                    .get(&handle)
                    .ok_or_else(|| ExecutionError::InvalidNodeHandle(handle.clone()))?,
            )?;
            self.join_handles.insert(handle.clone(), join_handle);
        }
        Ok(())
    }

    pub fn stop(&self) {
        self.stop_req
            .store(STOP_REQ_TERM_AND_SHUTDOWN, Ordering::Relaxed);
    }

    pub fn join(self) -> Result<(), HashMap<NodeHandle, ExecutionError>> {
        loop {
            let mut finished: usize = 0;
            for (_handle, thread) in self.join_handles.iter() {
                if thread.is_finished() {
                    let _ = self.stop_req.compare_exchange(
                        STOP_REQ_NO_ACTION,
                        STOP_REQ_SHUTDOWN,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    );
                    finished += 1;
                }
            }

            if finished == self.join_handles.len() {
                break;
            }

            thread::sleep(Duration::from_millis(250));
        }

        let mut results: HashMap<NodeHandle, ExecutionError> = HashMap::new();
        for (handle, t) in self.join_handles {
            match t.join() {
                Ok(Err(e)) => {
                    results.insert(handle.clone(), e);
                }
                Err(_e) => {
                    results.insert(handle.clone(), InternalThreadPanic);
                }
                _ => {}
            }
        }
        match results.is_empty() {
            true => Ok(()),
            false => Err(results),
        }
    }
}
