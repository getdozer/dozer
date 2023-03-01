use std::{borrow::Cow, collections::HashMap, mem::swap};

use crossbeam::channel::Receiver;
use daggy::NodeIndex;
use dozer_storage::lmdb_storage::SharedTransaction;
use dozer_types::node::NodeHandle;

use crate::{
    builder_dag::NodeKind,
    errors::ExecutionError,
    forwarder::{ProcessorChannelManager, StateWriter},
    node::{PortHandle, Processor},
    record_store::RecordReader,
};

use super::{
    execution_dag::ExecutionDag, name::Name, receiver_loop::ReceiverLoop, ExecutorOperation,
};

/// A processor in the execution DAG.
#[derive(Debug)]
pub struct ProcessorNode {
    /// Node handle in description DAG.
    node_handle: NodeHandle,
    /// Input port handles.
    port_handles: Vec<PortHandle>,
    /// Input data channels.
    receivers: Vec<Receiver<ExecutorOperation>>,
    /// The processor.
    processor: Box<dyn Processor>,
    /// Record readers of the input ports. Every record reader reads the state of corresponding output port.
    record_readers: HashMap<PortHandle, Box<dyn RecordReader>>,
    /// The transaction for this node's environment. Processor uses it to persist data.
    master_tx: SharedTransaction,
    /// This node's output channel manager, for forwarding data, writing metadata and writing port state.
    channel_manager: ProcessorChannelManager,
}

impl ProcessorNode {
    pub fn new(dag: &mut ExecutionDag, node_index: NodeIndex) -> Self {
        let node = dag.node_weight_mut(node_index);
        let node_handle = node.handle.clone();
        let node_storage = node.storage.clone();
        let Some(NodeKind::Processor(processor)) = node.kind.take() else {
            panic!("Must pass in a processor node");
        };

        let (port_handles, receivers, record_readers) =
            dag.collect_receivers_and_record_readers(node_index);

        let (senders, record_writers) = dag.collect_senders_and_record_writers(node_index);

        let state_writer = StateWriter::new(
            node_storage.meta_db,
            record_writers,
            node_storage.master_txn.clone(),
        );
        let channel_manager =
            ProcessorChannelManager::new(node_handle.clone(), senders, state_writer, true);

        Self {
            node_handle,
            port_handles,
            receivers,
            processor,
            record_readers,
            master_tx: node_storage.master_txn,
            channel_manager,
        }
    }

    pub fn handle(&self) -> &NodeHandle {
        &self.node_handle
    }
}

impl Name for ProcessorNode {
    fn name(&self) -> Cow<str> {
        Cow::Owned(self.node_handle.to_string())
    }
}

impl ReceiverLoop for ProcessorNode {
    fn receivers(&mut self) -> Vec<Receiver<ExecutorOperation>> {
        let mut result = vec![];
        swap(&mut self.receivers, &mut result);
        result
    }

    fn receiver_name(&self, index: usize) -> Cow<str> {
        Cow::Owned(self.port_handles[index].to_string())
    }

    fn on_op(
        &mut self,
        index: usize,
        op: dozer_types::types::Operation,
    ) -> Result<(), ExecutionError> {
        self.processor.process(
            self.port_handles[index],
            op,
            &mut self.channel_manager,
            &self.master_tx,
            &self.record_readers,
        )
    }

    fn on_commit(&mut self, epoch: &crate::epoch::Epoch) -> Result<(), ExecutionError> {
        self.processor.commit(epoch, &self.master_tx)?;
        self.channel_manager.store_and_send_commit(epoch)
    }

    fn on_terminate(&mut self) -> Result<(), ExecutionError> {
        self.channel_manager.send_terminate()
    }

    fn on_snapshotting_done(&mut self) -> Result<(), ExecutionError> {
        self.channel_manager.send_snapshotting_done()
    }
}
