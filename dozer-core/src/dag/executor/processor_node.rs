use std::{borrow::Cow, collections::HashMap, mem::swap, path::Path, sync::Arc};

use crossbeam::channel::{Receiver, Sender};
use dozer_types::parking_lot::RwLock;
use dozer_types::types::Schema;

use crate::{
    dag::{
        dag::Edge,
        errors::ExecutionError,
        executor_utils::{
            build_receivers_lists, create_ports_databases_and_fill_downstream_record_readers,
            init_component,
        },
        forwarder::{ProcessorChannelManager, StateWriter},
        node::{NodeHandle, PortHandle, Processor, ProcessorFactory},
        record_store::RecordReader,
    },
    storage::lmdb_storage::SharedTransaction,
};

use super::{name::Name, receiver_loop::ReceiverLoop, ExecutorOperation};

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
    /// Record readers of all stateful ports. Using `self.node_handle`, we can find the record readers of our stateful inputs.
    record_readers: Arc<RwLock<HashMap<NodeHandle, HashMap<PortHandle, Box<dyn RecordReader>>>>>,
    /// The transaction for this node's environment. Processor uses it to persist data.
    master_tx: SharedTransaction,
    /// This node's output channel manager, for forwarding data, writing metadata and writing port state.
    channel_manager: ProcessorChannelManager,
}

impl ProcessorNode {
    /// # Arguments
    ///
    /// - `node_handle`: Node handle in description DAG.
    /// - `processor_factory`: Processor factory in description DAG.
    /// - `base_path`: Base path of persisted data for the last execution of the description DAG.
    /// - `record_readers`: Record readers of all stateful ports.
    /// - `receivers`: Input channels to this processor.
    /// - `senders`: Output channels from this processor.
    /// - `edges`: All edges in the description DAG, used for creating record readers for input ports which is connected to this processor's stateful output ports.
    /// - `node_schemas`: Input and output data schemas.
    #[allow(clippy::too_many_arguments)]
    pub fn new<T: Clone>(
        node_handle: NodeHandle,
        processor_factory: &dyn ProcessorFactory<T>,
        base_path: &Path,
        record_readers: Arc<
            RwLock<HashMap<NodeHandle, HashMap<PortHandle, Box<dyn RecordReader>>>>,
        >,
        receivers: HashMap<PortHandle, Vec<Receiver<ExecutorOperation>>>,
        senders: HashMap<PortHandle, Vec<Sender<ExecutorOperation>>>,
        edges: &[Edge],
        input_schemas: HashMap<PortHandle, Schema>,
        output_schemas: HashMap<PortHandle, Schema>,
        retention_queue_size: usize,
    ) -> Result<Self, ExecutionError> {
        let mut processor = processor_factory.build(input_schemas, output_schemas.to_owned())?;
        let state_meta = init_component(&node_handle, base_path, |e| processor.init(e))?;

        let (master_tx, port_databases) =
            create_ports_databases_and_fill_downstream_record_readers(
                &node_handle,
                edges,
                state_meta.env,
                &processor_factory.get_output_ports(),
                &mut record_readers.write(),
            )?;
        let (port_handles, receivers) = build_receivers_lists(receivers);
        let channel_manager = ProcessorChannelManager::new(
            node_handle.clone(),
            senders,
            StateWriter::new(
                state_meta.meta_db,
                port_databases,
                master_tx.clone(),
                output_schemas,
                retention_queue_size,
            )?,
            true,
        );

        Ok(Self {
            node_handle,
            port_handles,
            receivers,
            processor,
            record_readers,
            master_tx,
            channel_manager,
        })
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
        let record_readers = self.record_readers.read();
        let reader = record_readers
            .get(&self.node_handle)
            .ok_or_else(|| ExecutionError::InvalidNodeHandle(self.node_handle.clone()))?;

        self.processor.process(
            self.port_handles[index],
            op,
            &mut self.channel_manager,
            &self.master_tx,
            reader,
        )
    }

    fn on_commit(&mut self, epoch: &crate::dag::epoch::Epoch) -> Result<(), ExecutionError> {
        self.processor.commit(epoch, &self.master_tx)?;
        self.channel_manager.store_and_send_commit(epoch)
    }

    fn on_terminate(&mut self) -> Result<(), ExecutionError> {
        self.channel_manager.send_terminate()
    }
}
