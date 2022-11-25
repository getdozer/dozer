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
use crossbeam::channel::{bounded, Receiver, RecvTimeoutError, Sender};
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
        let exec_op = map_to_exec_op(seq, op);
        internal_err!(sender.send(exec_op))
    }

    fn update_schema(&mut self, schema: Schema, port: PortHandle) -> Result<(), ExecutionError> {
        let sender = self
            .senders
            .get(&port)
            .ok_or(ExecutionError::InvalidPortHandle(port))?;
        internal_err!(sender.send(ExecutorOperation::SchemaUpdate { new: schema }))
    }

    fn terminate(&mut self) -> Result<(), ExecutionError> {
        for sender in &self.senders {
            let _ = sender.1.send(ExecutorOperation::Terminate);
        }
        Ok(())
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
    let mut internal_receivers: Vec<Receiver<ExecutorOperation>> = Vec::new();
    let mut internal_senders: HashMap<PortHandle, Sender<ExecutorOperation>> = HashMap::new();
    let output_ports = src_factory.get_output_ports();

    for port in &output_ports {
        let channels = bounded::<ExecutorOperation>(channel_buffer);
        internal_receivers.push(channels.1);
        internal_senders.insert(port.handle, channels.0);
    }

    let source_thread_latch = Arc::new(CountDownLatch::new(1));
    let source_thread_latch_thread = source_thread_latch.clone();
    let mut fw = InternalChannelSourceForwarder::new(internal_senders);
    let sender_handle = handle.clone();
    let sender_thread = thread::spawn(move || -> Result<(), ExecutionError> {
        info!("SRC [{}::sender] Starting up...", sender_handle);
        let src = src_factory.build();
        for p in src_factory.get_output_ports() {
            if let Some(schema) = src.get_output_schema(p.handle) {
                fw.update_schema(schema, p.handle)?
            }
        }
        source_thread_latch_thread.countdown();
        let r = src.start(&mut fw, None);
        match &r {
            Err(e) => {
                error!(
                    "SRC [{}::sender] Exiting with error {}",
                    sender_handle,
                    e.to_string()
                );
            }
            Ok(()) => {
                info!("SRC [{}::sender] Exiting without error", sender_handle);
            }
        }
        r
    });
    source_thread_latch.wait();

    let listener_handle = handle.clone();
    thread::spawn(move || -> Result<(), ExecutionError> {
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
        let mut sel = init_select(&internal_receivers);
        loop {
            let port_index = sel.ready();
            let r = internal_receivers[port_index]
                .recv_deadline(Instant::now().add(Duration::from_millis(500)));

            if sender_thread.is_finished() {
                info!(
                    "SRC [{}] Sender thread has exited. Requesting for shutdown...",
                    &listener_handle
                );
                stop_req.store(true, Ordering::Relaxed);
            }

            if process_message(
                &stop_req,
                &listener_handle,
                r,
                &mut dag_fw,
                output_ports[port_index].handle,
                true,
                &term_barrier,
            )? {
                return Ok(());
            }
        }
    })
}
