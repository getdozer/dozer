use std::sync::Arc;
use std::{borrow::Cow, mem::swap};

use crossbeam::channel::Receiver;
use daggy::NodeIndex;
use dozer_types::epoch::Epoch;
use dozer_types::node::NodeHandle;

use crate::error_manager::ErrorManager;
use crate::executor_operation::{ExecutorOperation, ProcessorOperation};
use crate::processor_record::ProcessorRecordStore;
use crate::{
    builder_dag::NodeKind,
    errors::ExecutionError,
    forwarder::ProcessorChannelManager,
    node::{PortHandle, Processor},
};

use super::{execution_dag::ExecutionDag, name::Name, receiver_loop::ReceiverLoop};

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
    /// This node's output channel manager, for forwarding data, writing metadata and writing port state.
    channel_manager: ProcessorChannelManager,
    /// Where all the records from ingested data are stored.
    record_store: Arc<ProcessorRecordStore>,
    /// The error manager, for reporting non-fatal errors.
    error_manager: Arc<ErrorManager>,
}

impl ProcessorNode {
    pub fn new(dag: &mut ExecutionDag, node_index: NodeIndex) -> Self {
        let Some(node) = dag.node_weight_mut(node_index).take() else {
            panic!("Must pass in a node")
        };
        let node_handle = node.handle;
        let NodeKind::Processor(processor) = node.kind else {
            panic!("Must pass in a processor node");
        };

        let (port_handles, receivers) = dag.collect_receivers(node_index);

        let (senders, _) = dag.collect_senders_and_record_writers(node_index);

        let channel_manager = ProcessorChannelManager::new(
            node_handle.clone(),
            senders,
            None,
            dag.error_manager().clone(),
        );

        Self {
            node_handle,
            port_handles,
            receivers,
            processor,
            channel_manager,
            record_store: dag.record_store().clone(),
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
    fn receivers(&mut self) -> Vec<Receiver<ExecutorOperation>> {
        let mut result = vec![];
        swap(&mut self.receivers, &mut result);
        result
    }

    fn receiver_name(&self, index: usize) -> Cow<str> {
        Cow::Owned(self.port_handles[index].to_string())
    }

    fn on_op(&mut self, index: usize, op: ProcessorOperation) -> Result<(), ExecutionError> {
        if let Err(e) = self.processor.process(
            self.port_handles[index],
            &self.record_store,
            op,
            &mut self.channel_manager,
        ) {
            self.error_manager.report(e);
        }
        Ok(())
    }

    fn on_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError> {
        if let Err(e) = self.processor.commit(epoch) {
            self.error_manager.report(e);
        }
        self.channel_manager.store_and_send_commit(epoch)
    }

    fn on_terminate(&mut self) -> Result<(), ExecutionError> {
        self.channel_manager.send_terminate()
    }

    fn on_snapshotting_done(&mut self, connection_name: String) -> Result<(), ExecutionError> {
        self.channel_manager.send_snapshotting_done(connection_name)
    }
}
