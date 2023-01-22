use std::{
    collections::HashMap,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use crossbeam::channel::{Receiver, RecvTimeoutError, Sender};
use dozer_types::log::debug;
use dozer_types::{
    internal_err,
    parking_lot::RwLock,
    types::{Operation, Schema},
};

use crate::dag::{
    channels::SourceChannelForwarder,
    dag::Edge,
    epoch::EpochManager,
    errors::ExecutionError::{self, InternalError},
    executor_utils::{create_ports_databases_and_fill_downstream_record_readers, init_component},
    forwarder::{SourceChannelManager, StateWriter},
    node::{NodeHandle, OutputPortDef, PortHandle, Source, SourceFactory},
    record_store::RecordReader,
};

use super::{node::Node, ExecutorOperation};

#[derive(Debug)]
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

/// The sender half of a source in the execution DAG.
#[derive(Debug)]
pub struct SourceSenderNode {
    /// Node handle in description DAG.
    node_handle: NodeHandle,
    /// The source.
    source: Box<dyn Source>,
    /// Last checkpointed output data sequence number.
    last_checkpoint: (u64, u64),
    /// The forwarder that will be passed to the source for outputig data.
    forwarder: InternalChannelSourceForwarder,
    /// If the execution DAG should be running. Used for terminating the execution DAG.
    running: Arc<AtomicBool>,
}

impl SourceSenderNode {
    /// # Arguments
    ///
    /// - `node_handle`: Node handle in description DAG.
    /// - `source_factory`: Source factory in description DAG.
    /// - `output_schemas`: Output data schemas.
    /// - `last_checkpoint`: Last checkpointed output of this source.
    /// - `sender`: Channel to send data to.
    /// - `running`: If the execution DAG should still be running.
    pub fn new<T>(
        node_handle: NodeHandle,
        source_factory: &dyn SourceFactory<T>,
        output_schemas: HashMap<PortHandle, Schema>,
        last_checkpoint: (u64, u64),
        sender: Sender<(PortHandle, u64, u64, Operation)>,
        running: Arc<AtomicBool>,
    ) -> Result<Self, ExecutionError> {
        let source = source_factory.build(output_schemas)?;
        let forwarder = InternalChannelSourceForwarder::new(sender);
        Ok(Self {
            node_handle,
            source,
            last_checkpoint,
            forwarder,
            running,
        })
    }
}

impl Node for SourceSenderNode {
    fn run(mut self) -> Result<(), ExecutionError> {
        let result = self
            .source
            .start(&mut self.forwarder, Some(self.last_checkpoint));
        self.running.store(false, Ordering::SeqCst);
        debug!("[{}-sender] Quit", self.node_handle);
        result
    }
}

/// The listener part of a source in the execution DAG.
#[derive(Debug)]
pub struct SourceListenerNode {
    /// Node handle in description DAG.
    node_handle: NodeHandle,
    /// Output from corresponding source sender.
    receiver: Receiver<(PortHandle, u64, u64, Operation)>,
    /// Receiving timeout.
    timeout: Duration,
    /// If the execution DAG should be running. Used for determining if a `terminate` message should be sent.
    running: Arc<AtomicBool>,
    /// This node's output channel manager, for communicating to other sources to coordinate terminate and commit, forwarding data, writing metadata and writing port state.
    channel_manager: SourceChannelManager,
}

impl SourceListenerNode {
    /// # Arguments
    ///
    /// - `node_handle`: Node handle in description DAG.
    /// - `receiver`: Channel that the data comes in.
    /// - `timeout`: `Listener timeout. After this timeout, listener will check if commit or terminate need to happen.
    /// - `base_path`: Base path of persisted data for the last execution of the description DAG.
    /// - `output_ports`: Output port definition of the source in description DAG.
    /// - `record_readers`: Record readers of all stateful ports.
    /// - `senders`: Output channels from this processor.
    /// - `edges`: All edges in the description DAG, used for creating record readers for input ports which is connected to this processor's stateful output ports.
    /// - `running`: If the execution DAG should still be running.
    /// - `epoch_manager`: Used for coordinating commit and terminate between sources. Shared by all sources.
    /// - `output_schemas`: Output data schemas.
    /// - `retention_queue_size`: Size of retention queue (used by RecordWriter)
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        node_handle: NodeHandle,
        receiver: Receiver<(PortHandle, u64, u64, Operation)>,
        timeout: Duration,
        base_path: &Path,
        output_ports: &[OutputPortDef],
        record_readers: Arc<
            RwLock<HashMap<NodeHandle, HashMap<PortHandle, Box<dyn RecordReader>>>>,
        >,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        edges: &[Edge],
        running: Arc<AtomicBool>,
        commit_sz: u32,
        max_duration_between_commits: Duration,
        epoch_manager: Arc<EpochManager>,
        output_schemas: HashMap<PortHandle, Schema>,
        start_seq: (u64, u64),
        retention_queue_size: usize,
    ) -> Result<Self, ExecutionError> {
        let state_meta = init_component(&node_handle, base_path, |_| Ok(()))?;
        let (master_tx, port_databases) =
            create_ports_databases_and_fill_downstream_record_readers(
                &node_handle,
                edges,
                state_meta.env,
                output_ports,
                &mut record_readers.write(),
            )?;
        let channel_manager = SourceChannelManager::new(
            node_handle.clone(),
            senders,
            StateWriter::new(
                state_meta.meta_db,
                port_databases,
                master_tx,
                output_schemas,
                retention_queue_size,
            )?,
            true,
            commit_sz,
            max_duration_between_commits,
            epoch_manager,
            start_seq,
        );
        Ok(Self {
            node_handle,
            receiver,
            timeout,
            running,
            channel_manager,
        })
    }
}

impl SourceListenerNode {
    /// Returns if the node should terminate.
    fn send_and_trigger_commit_if_needed(
        &mut self,
        data: Option<(PortHandle, u64, u64, Operation)>,
    ) -> Result<bool, ExecutionError> {
        // First check if termination was requested.
        let terminating = !self.running.load(Ordering::SeqCst);
        // If this commit was not requested with termination at the start, we shouldn't terminate either.
        let terminating = match data {
            Some((port, txid, seq_in_tx, op)) => self
                .channel_manager
                .send_and_trigger_commit_if_needed(txid, seq_in_tx, op, port, terminating)?,
            None => self.channel_manager.trigger_commit_if_needed(terminating)?,
        };
        if terminating {
            self.channel_manager.terminate()?;
            debug!("[{}-listener] Quitting", &self.node_handle);
        }
        Ok(terminating)
    }
}

impl Node for SourceListenerNode {
    fn run(mut self) -> Result<(), ExecutionError> {
        loop {
            match self.receiver.recv_timeout(self.timeout) {
                Ok(data) => {
                    if self.send_and_trigger_commit_if_needed(Some(data))? {
                        return Ok(());
                    }
                }
                Err(e) => {
                    if self.send_and_trigger_commit_if_needed(None)? {
                        return Ok(());
                    }
                    // Channel disconnected but running flag not set to false, the source sender must have panicked.
                    if self.running.load(Ordering::SeqCst) && e == RecvTimeoutError::Disconnected {
                        return Err(ExecutionError::ChannelDisconnected);
                    }
                }
            }
        }
    }
}
