use std::sync::Arc;
use std::{borrow::Cow, mem::swap};

use crossbeam::channel::Receiver;
use daggy::NodeIndex;
use dozer_types::node::{NodeHandle, OpIdentifier};
use dozer_types::types::OperationWithId;

use crate::epoch::Epoch;
use crate::error_manager::ErrorManager;
use crate::executor_operation::ExecutorOperation;
use crate::{
    builder_dag::NodeKind,
    errors::ExecutionError,
    forwarder::ChannelManager,
    node::{PortHandle, Processor},
};

use super::{execution_dag::ExecutionDag, name::Name, receiver_loop::ReceiverLoop};

/// A processor in the execution DAG.
#[derive(Debug)]
pub struct ProcessorNode {
    /// Node handle in description DAG.
    node_handle: NodeHandle,
    /// The epoch id the processor was constructed for.
    initial_epoch_id: u64,
    /// Input port handles.
    port_handles: Vec<PortHandle>,
    /// Input data channels.
    receivers: Vec<Receiver<ExecutorOperation>>,
    /// The processor.
    processor: Box<dyn Processor>,
    /// This node's output channel manager, for forwarding data, writing metadata and writing port state.
    channel_manager: ChannelManager,
    /// The error manager, for reporting non-fatal errors.
    error_manager: Arc<ErrorManager>,
}

impl ProcessorNode {
    pub async fn new(dag: &mut ExecutionDag, node_index: NodeIndex) -> Self {
        let Some(node) = dag.node_weight_mut(node_index).take() else {
            panic!("Must pass in a node")
        };
        let node_handle = node.handle;
        let NodeKind::Processor(processor) = node.kind else {
            panic!("Must pass in a processor node");
        };

        let (port_handles, receivers) = dag.collect_receivers(node_index);

        let (senders, record_writers) = dag.collect_senders_and_record_writers(node_index).await;

        let channel_manager = ChannelManager::new(
            node_handle.clone(),
            record_writers,
            senders,
            dag.error_manager().clone(),
        );

        Self {
            node_handle,
            initial_epoch_id: dag.epoch_manager().epoch_id(),
            port_handles,
            receivers,
            processor,
            channel_manager,
            error_manager: dag.error_manager().clone(),
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
    fn initial_epoch_id(&self) -> u64 {
        self.initial_epoch_id
    }

    fn receivers(&mut self) -> Vec<Receiver<ExecutorOperation>> {
        let mut result = vec![];
        swap(&mut self.receivers, &mut result);
        result
    }

    fn receiver_name(&self, index: usize) -> Cow<str> {
        Cow::Owned(self.port_handles[index].to_string())
    }

    fn on_op(&mut self, index: usize, op: OperationWithId) -> Result<(), ExecutionError> {
        if let Err(e) =
            self.processor
                .process(self.port_handles[index], op, &mut self.channel_manager)
        {
            self.error_manager.report(e);
        }
        Ok(())
    }

    fn on_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError> {
        if let Err(e) = self.processor.commit(epoch) {
            self.error_manager.report(e);
        }

        if let Some(checkpoint_writer) = &epoch.common_info.checkpoint_writer {
            let object = checkpoint_writer.create_processor_object(&self.node_handle)?;
            self.processor
                .serialize(object)
                .map_err(ExecutionError::FailedToCreateCheckpoint)?;
        }

        self.channel_manager.send_commit(epoch)
    }

    fn on_terminate(&mut self) -> Result<(), ExecutionError> {
        self.channel_manager.send_terminate()
    }

    fn on_snapshotting_started(&mut self, connection_name: String) -> Result<(), ExecutionError> {
        self.channel_manager
            .send_snapshotting_started(connection_name)
    }

    fn on_snapshotting_done(
        &mut self,
        connection_name: String,
        id: Option<OpIdentifier>,
    ) -> Result<(), ExecutionError> {
        self.channel_manager
            .send_snapshotting_done(connection_name, id)
    }
}
