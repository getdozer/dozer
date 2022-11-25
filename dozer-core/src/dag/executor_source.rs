#![allow(clippy::too_many_arguments)]
use crate::dag::channels::SourceChannelForwarder;
use crate::dag::dag::Edge;
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::InternalError;
use crate::dag::executor_local::ExecutorOperation;
use crate::dag::executor_utils::{
    create_ports_databases, fill_ports_record_readers, init_component, init_select, map_to_exec_op,
};
use crate::dag::forwarder::{LocalChannelForwarder, StateWriter};
use crate::dag::node::{NodeHandle, PortHandle, SourceFactory};
use crate::dag::record_store::RecordReader;
use crate::storage::common::RenewableRwTransaction;
use crossbeam::channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender};
use dozer_types::internal_err;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Schema};
use fp_rust::sync::CountDownLatch;
use log::{error, info, warn};
use std::collections::HashMap;
use std::ops::Add;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

enum SourceState {
    WaitingForSchema,
    Running,
}

#[derive(Debug)]
pub(crate) struct SourceExecutorOperation {
    port: PortHandle,
    op: ExecutorOperation,
}

impl SourceExecutorOperation {
    pub fn new(port: PortHandle, op: ExecutorOperation) -> Self {
        Self { port, op }
    }
}

pub(crate) struct InternalChannelSourceForwarder {
    sender: Arc<Sender<SourceExecutorOperation>>,
}

impl InternalChannelSourceForwarder {
    pub fn new(sender: Arc<Sender<SourceExecutorOperation>>) -> Self {
        Self { sender }
    }
}

impl SourceChannelForwarder for InternalChannelSourceForwarder {
    fn send(&mut self, seq: u64, op: Operation, port: PortHandle) -> Result<(), ExecutionError> {
        internal_err!(self
            .sender
            .send(SourceExecutorOperation::new(port, map_to_exec_op(seq, op))))
    }

    fn update_schema(&mut self, schema: Schema, port: PortHandle) -> Result<(), ExecutionError> {
        internal_err!(self.sender.send(SourceExecutorOperation::new(
            port,
            ExecutorOperation::SchemaUpdate { new: schema }
        )))
    }
}

fn process_message(
    stop_req: &Arc<AtomicBool>,
    owner: &NodeHandle,
    r: Result<ExecutorOperation, RecvTimeoutError>,
    dag_fw: &mut LocalChannelForwarder,
    port: PortHandle,
    _stateful: bool,
    term_barrier: &Arc<Barrier>,
) -> Result<bool, ExecutionError> {
    if stop_req.load(Ordering::Relaxed) {
        info!("SRC [{}] Shutdown requested. Sending TERM request", owner);
        dag_fw.terminate()?;
        term_barrier.wait();
        return Ok(true);
    }

    match r {
        Err(RecvTimeoutError::Timeout) => {
            dag_fw.trigger_commit_if_needed()?;
            Ok(false)
        }
        Err(RecvTimeoutError::Disconnected) => {
            warn!("SRC [{}] Source exited. Shutting down...", owner);
            Ok(true)
        }
        Ok(ExecutorOperation::Insert { seq, new }) => {
            dag_fw.send(seq, Operation::Insert { new }, port)?;
            Ok(false)
        }
        Ok(ExecutorOperation::Delete { seq, old }) => {
            dag_fw.send(seq, Operation::Delete { old }, port)?;
            Ok(false)
        }
        Ok(ExecutorOperation::Update { seq, old, new }) => {
            dag_fw.send(seq, Operation::Update { old, new }, port)?;
            Ok(false)
        }
        Ok(ExecutorOperation::SchemaUpdate { new }) => {
            dag_fw.update_schema(new, port)?;
            Ok(false)
        }
        Ok(ExecutorOperation::Terminate) => {
            dag_fw.terminate()?;
            term_barrier.wait();
            Ok(true)
        }
        _ => Ok(false),
    }
}

pub(crate) fn start_source_thread(
    handle: NodeHandle,
    mut fw: InternalChannelSourceForwarder,
    src_factory: Box<dyn SourceFactory>,
) -> JoinHandle<Result<(), ExecutionError>> {
    //
    thread::spawn(move || -> Result<(), ExecutionError> {
        info!("SRC [{}::sender] Starting up...", handle);
        let src = src_factory.build();
        for p in src_factory.get_output_ports() {
            if let Some(schema) = src.get_output_schema(p.handle) {
                fw.update_schema(schema, p.handle)?
            }
        }
        let r = src.start(&mut fw, None);
        match &r {
            Err(e) => {
                error!(
                    "SRC [{}::sender] Exiting with error {}",
                    handle,
                    e.to_string()
                );
            }
            Ok(()) => {
                info!("SRC [{}::sender] Exiting without error", handle);
            }
        }
        r
    })
}

pub(crate) fn start_source(
    stop_req: Arc<AtomicBool>,
    edges: Vec<Edge>,
    handle: NodeHandle,
    src_factory: Box<dyn SourceFactory>,
    out_channels: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
    commit_size: u32,
    commit_time: Duration,
    channel_buffer: usize,
    record_stores: Arc<RwLock<HashMap<NodeHandle, HashMap<PortHandle, RecordReader>>>>,
    base_path: PathBuf,
    term_barrier: Arc<Barrier>,
) -> JoinHandle<Result<(), ExecutionError>> {
    //
    let output_ports = src_factory.get_output_ports();
    let (src_snd, src_rcv) = bounded::<SourceExecutorOperation>(channel_buffer);
    let shared_src_snd = Arc::new(src_snd);

    let mut fw = InternalChannelSourceForwarder::new(shared_src_snd.clone());
    let sender_thread = start_source_thread(handle.clone(), fw, src_factory);

    let listener_handle = handle.clone();

    thread::spawn(move || -> Result<(), ExecutionError> {
        //
        let _output_schemas = HashMap::<PortHandle, Schema>::new();
        let mut state_meta = init_component(&handle, base_path.as_path(), |_e| Ok(()))?;

        let port_databases =
            create_ports_databases(state_meta.env.as_environment(), &output_ports)?;

        let master_tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>> =
            Arc::new(RwLock::new(state_meta.env.create_txn()?));

        fill_ports_record_readers(
            &handle,
            &edges,
            &port_databases,
            &master_tx,
            &record_stores,
            &output_ports,
        );

        let mut dag_fw = LocalChannelForwarder::new_source_forwarder(
            handle,
            out_channels,
            commit_size,
            commit_time,
            StateWriter::new(state_meta.meta_db, port_databases, master_tx.clone(), None),
            true,
        );

        let mut state = SourceState::WaitingForSchema;
        loop {
            let res = src_rcv.recv_deadline(Instant::now().add(Duration::from_millis(500)));
            match state {
                SourceState::WaitingForSchema => {
                    if stop_req.load(Ordering::Relaxed)
                        || sender_thread.is_finished()
                        || (res.is_err()
                            && res.as_ref().unwrap_err() == &RecvTimeoutError::Disconnected)
                    {
                        stop_req.store(true, Ordering::Relaxed);
                        term_barrier.wait();
                        return Ok(());
                    } else if let Ok(s_op) = res {
                        match s_op.op {
                            ExecutorOperation::SchemaUpdate { new } => {
                                dag_fw.update_schema(new, s_op.port)?;
                                state = SourceState::Running;
                                continue;
                            }
                            _ => {
                                stop_req.store(true, Ordering::Relaxed);
                                term_barrier.wait();
                                return Err(ExecutionError::InvalidOperation("Wrong operation received while waiting for schema initialization".to_string()));
                            }
                        }
                    }
                }
                SourceState::Running => {
                    match res {
                        Err(RecvTimeoutError::Timeout) => {
                            dag_fw.trigger_commit_if_needed()?;
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            stop_req.store(true, Ordering::Relaxed);
                            dag_fw.terminate()?;
                            term_barrier.wait();
                            return Ok(());
                        }
                        Ok(SourceExecutorOperation {
                            port,
                            op: ExecutorOperation::SchemaUpdate { new },
                        }) => dag_fw.update_schema(new, port)?,
                        Ok(SourceExecutorOperation {
                            port,
                            op: ExecutorOperation::Insert { seq, new },
                        }) => dag_fw.send(seq, Operation::Insert { new }, port)?,
                        Ok(SourceExecutorOperation {
                            port,
                            op: ExecutorOperation::Delete { seq, old },
                        }) => dag_fw.send(seq, Operation::Delete { old }, port)?,
                        Ok(SourceExecutorOperation {
                            port,
                            op: ExecutorOperation::Update { seq, old, new },
                        }) => dag_fw.send(seq, Operation::Update { old, new }, port)?,
                        Ok(SourceExecutorOperation {
                            port,
                            op: ExecutorOperation::Terminate,
                        }) => {
                            dag_fw.terminate()?;
                            term_barrier.wait();
                            return Ok(());
                        }
                        _ => {}
                    }

                    if (stop_req.load(Ordering::Relaxed) || sender_thread.is_finished()) {
                        stop_req.store(true, Ordering::Relaxed);
                        dag_fw.terminate()?;
                        term_barrier.wait();
                        return Ok(());
                    }
                }
            }
        }
    })
}
