use std::{borrow::Cow, collections::HashMap, mem::swap, path::Path, sync::Arc};

use crossbeam::channel::Receiver;
use dozer_types::log::debug;
use dozer_types::{parking_lot::RwLock, types::Schema};

use crate::{
    dag::{
        epoch::Epoch,
        errors::ExecutionError,
        executor_utils::{build_receivers_lists, init_component},
        forwarder::StateWriter,
        node::{NodeHandle, PortHandle, Sink, SinkFactory},
        record_store::RecordReader,
    },
    storage::lmdb_storage::SharedTransaction,
};

use super::{name::Name, receiver_loop::ReceiverLoop, ExecutorOperation};

/// A sink in the execution DAG.
#[derive(Debug)]
pub struct SinkNode {
    /// Node handle in description DAG.
    node_handle: NodeHandle,
    /// Input port handles.
    port_handles: Vec<u16>,
    /// Input data channels.
    receivers: Vec<Receiver<ExecutorOperation>>,
    /// The sink.
    sink: Box<dyn Sink>,
    /// Record readers of all stateful ports. Using `self.node_handle`, we can find the record readers of our stateful inputs.
    record_readers: Arc<RwLock<HashMap<NodeHandle, HashMap<PortHandle, Box<dyn RecordReader>>>>>,
    /// The transaction for this node's environment. Sink uses it to persist data.
    master_tx: SharedTransaction,
    /// This node's state writer, for writing metadata and port state.
    state_writer: StateWriter,
}

impl SinkNode {
    /// # Arguments
    ///
    /// - `node_handle`: Node handle in description DAG.
    /// - `sink_factory`: Sink factory in description DAG.
    /// - `base_path`: Base path of persisted data for the last execution of the description DAG.
    /// - `record_readers`: Record readers of all stateful ports.
    /// - `receivers`: Input channels to this sink.
    /// - `input_schemas`: Input data schemas.
    pub fn new<T: Clone>(
        node_handle: NodeHandle,
        sink_factory: &dyn SinkFactory<T>,
        base_path: &Path,
        record_readers: Arc<
            RwLock<HashMap<NodeHandle, HashMap<PortHandle, Box<dyn RecordReader>>>>,
        >,
        receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
        input_schemas: HashMap<PortHandle, Schema>,
        retention_queue_size: usize,
    ) -> Result<Self, ExecutionError> {
        let mut sink = sink_factory.build(input_schemas)?;
        let state_meta = init_component(&node_handle, base_path, |e| sink.init(e))?;
        let master_tx = state_meta.env.create_txn()?;
        let state_writer = StateWriter::new(
            state_meta.meta_db,
            HashMap::new(),
            master_tx.clone(),
            HashMap::new(),
            retention_queue_size,
        )?;
        let (port_handles, receivers) = build_receivers_lists(receivers);
        Ok(Self {
            node_handle,
            port_handles,
            receivers,
            sink,
            record_readers,
            master_tx,
            state_writer,
        })
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
        let record_readers = self.record_readers.read();
        let reader = record_readers
            .get(&self.node_handle)
            .ok_or_else(|| ExecutionError::InvalidNodeHandle(self.node_handle.clone()))?;
        self.sink
            .process(self.port_handles[index], op, &self.master_tx, reader)
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
