use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use crossbeam::channel::{bounded, Receiver, RecvTimeoutError, Sender};
use dozer_types::types::Operation;
use dozer_types::{
    log::debug,
    node::{NodeHandle, OpIdentifier},
};

use crate::{
    channels::SourceChannelForwarder,
    errors::ExecutionError,
    forwarder::{SourceChannelManager, StateWriter},
    node::{PortHandle, Source},
};

use super::{
    execution_dag::{ExecutionDag, NodeKind},
    node::Node,
    ExecutorOptions,
};

impl SourceChannelForwarder for InternalChannelSourceForwarder {
    fn send(
        &mut self,
        txid: u64,
        seq_in_tx: u64,
        op: Operation,
        port: PortHandle,
    ) -> Result<(), ExecutionError> {
        Ok(self.sender.send((port, txid, seq_in_tx, op))?)
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
    last_checkpoint: Option<OpIdentifier>,
    /// The forwarder that will be passed to the source for outputting data.
    forwarder: InternalChannelSourceForwarder,
}

impl SourceSenderNode {
    pub fn handle(&self) -> &NodeHandle {
        &self.node_handle
    }
}

impl Node for SourceSenderNode {
    fn run(mut self) -> Result<(), ExecutionError> {
        let result = self.source.start(
            &mut self.forwarder,
            self.last_checkpoint
                .map(|op_id| (op_id.txid, op_id.seq_in_tx)),
        );
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

#[derive(Debug, Clone, PartialEq)]
enum DataKind {
    Data((PortHandle, u64, u64, Operation)),
    NoDataBecauseOfTimeout,
    NoDataBecauseOfChannelDisconnection,
}

impl SourceListenerNode {
    /// Returns if the node should terminate.
    fn send_and_trigger_commit_if_needed(
        &mut self,
        data: DataKind,
    ) -> Result<bool, ExecutionError> {
        // If termination was requested the or source quit, we try to terminate.
        let terminating = data == DataKind::NoDataBecauseOfChannelDisconnection
            || !self.running.load(Ordering::SeqCst);
        // If this commit was not requested with termination at the start, we shouldn't terminate either.
        let terminating = match data {
            DataKind::Data((port, txid, seq_in_tx, op)) => self
                .channel_manager
                .send_and_trigger_commit_if_needed(txid, seq_in_tx, op, port, terminating)?,
            DataKind::NoDataBecauseOfTimeout | DataKind::NoDataBecauseOfChannelDisconnection => {
                self.channel_manager.trigger_commit_if_needed(terminating)?
            }
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
            let terminating = match self.receiver.recv_timeout(self.timeout) {
                Ok(data) => self.send_and_trigger_commit_if_needed(DataKind::Data(data))?,
                Err(RecvTimeoutError::Timeout) => {
                    self.send_and_trigger_commit_if_needed(DataKind::NoDataBecauseOfTimeout)?
                }
                Err(RecvTimeoutError::Disconnected) => self.send_and_trigger_commit_if_needed(
                    DataKind::NoDataBecauseOfChannelDisconnection,
                )?,
            };
            if terminating {
                return Ok(());
            }
        }
    }
}

#[derive(Debug)]
struct InternalChannelSourceForwarder {
    sender: Sender<(PortHandle, u64, u64, Operation)>,
}

impl InternalChannelSourceForwarder {
    pub fn new(sender: Sender<(PortHandle, u64, u64, Operation)>) -> Self {
        Self { sender }
    }
}

pub fn create_source_nodes(
    dag: &mut ExecutionDag,
    node_index: daggy::NodeIndex,
    options: &ExecutorOptions,
    running: Arc<AtomicBool>,
) -> (SourceSenderNode, SourceListenerNode) {
    // Get the source node.
    let node = dag.node_weight_mut(node_index);
    let node_handle = node.handle.clone();
    let node_storage = node.storage.clone();
    let Some(NodeKind::Source(source, last_checkpoint)) = node.kind.take() else {
        panic!("Must pass in a source node");
    };

    // Create channel between source sender and source listener.
    let (source_sender, source_receiver) = bounded(options.channel_buffer_sz);
    // let (source_sender, source_receiver) = bounded(1);

    // Create source listener.
    let forwarder = InternalChannelSourceForwarder::new(source_sender);
    let source_sender_node = SourceSenderNode {
        node_handle: node_handle.clone(),
        source,
        last_checkpoint,
        forwarder,
    };

    // Create source sender node.
    let (senders, record_writers) = dag.collect_senders_and_record_writers(node_index);
    let state_writer = StateWriter::new(
        node_storage.meta_db,
        record_writers,
        node_storage.master_txn,
    );
    let channel_manager = SourceChannelManager::new(
        node_handle.clone(),
        senders,
        state_writer,
        true,
        options.commit_sz,
        options.commit_time_threshold,
        dag.epoch_manager().clone(),
    );
    let source_listener_node = SourceListenerNode {
        node_handle,
        receiver: source_receiver,
        timeout: options.commit_time_threshold,
        running,
        channel_manager,
    };

    (source_sender_node, source_listener_node)
}
