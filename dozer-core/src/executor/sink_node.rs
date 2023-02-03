use std::{borrow::Cow, collections::HashMap, mem::swap};

use crossbeam::channel::Receiver;
use daggy::NodeIndex;
use dozer_storage::lmdb_storage::SharedTransaction;
use dozer_types::log::debug;

use crate::{
    epoch::Epoch,
    errors::ExecutionError,
    forwarder::StateWriter,
    node::{NodeHandle, PortHandle, Sink},
    record_store::RecordReader,
};

use super::execution_dag::{ExecutionDag, NodeKind};
use super::{name::Name, receiver_loop::ReceiverLoop, ExecutorOperation};

/// A sink in the execution DAG.
#[derive(Debug)]
pub struct SinkNode {
    /// Node handle in description DAG.
    node_handle: NodeHandle,
    /// Input port handles.
    port_handles: Vec<PortHandle>,
    /// Input data channels.
    receivers: Vec<Receiver<ExecutorOperation>>,
    /// The sink.
    sink: Box<dyn Sink>,
    /// Record readers of the input ports. Every record reader reads the state of corresponding output port.
    record_readers: HashMap<PortHandle, Box<dyn RecordReader>>,
    /// The transaction for this node's environment. Sink uses it to persist data.
    master_tx: SharedTransaction,
    /// This node's state writer, for writing metadata and port state.
    state_writer: StateWriter,
}

impl SinkNode {
    pub fn new(dag: &mut ExecutionDag, node_index: NodeIndex) -> Self {
        let node = &mut dag.graph_mut()[node_index];
        let (node_storage, Some(NodeKind::Sink(sink))) = (node.storage.clone(), node.kind.take()) else {
            panic!("Must pass in a sink node");
        };

        let (port_handles, receivers, record_readers) =
            dag.collect_receivers_and_record_readers(node_index);

        let state_writer = StateWriter::new(
            node_storage.meta_db,
            HashMap::new(),
            node_storage.master_tx.clone(),
        );

        Self {
            node_handle: node_storage.handle,
            port_handles,
            receivers,
            sink,
            record_readers,
            master_tx: node_storage.master_tx,
            state_writer,
        }
    }

    pub fn handle(&self) -> &NodeHandle {
        &self.node_handle
    }
}

impl Name for SinkNode {
    fn name(&self) -> Cow<str> {
        Cow::Owned(self.node_handle.to_string())
    }
}

impl ReceiverLoop for SinkNode {
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
        self.sink.process(
            self.port_handles[index],
            op,
            &self.master_tx,
            &self.record_readers,
        )
    }

    fn on_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError> {
        debug!("[{}] Checkpointing - {}", self.node_handle, epoch);
        self.sink.commit(epoch, &self.master_tx)?;
        self.state_writer.store_commit_info(epoch)
    }

    fn on_terminate(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }
}
