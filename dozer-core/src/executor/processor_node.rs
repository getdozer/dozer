use std::{borrow::Cow, mem::swap};

use crossbeam::channel::Receiver;
use daggy::NodeIndex;
use dozer_types::epoch::Epoch;
use dozer_types::node::NodeHandle;
use dozer_types::{epoch::ExecutorOperation, log::warn};

use crate::{
    builder_dag::NodeKind,
    errors::ExecutionError,
    forwarder::{ProcessorChannelManager, StateWriter},
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

        let (senders, record_writers) = dag.collect_senders_and_record_writers(node_index);

        let state_writer = StateWriter::new(record_writers);
        let channel_manager =
            ProcessorChannelManager::new(node_handle.clone(), senders, state_writer, true);

        Self {
            node_handle,
            port_handles,
            receivers,
            processor,
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
        let result =
            self.processor
                .process(self.port_handles[index], op, &mut self.channel_manager);
        if let Err(e) = result {
            warn!("Processor error: {:?}", e);
        }

        // TODO: Enable "test_run_dag_proc_err_2" and "test_run_dag_proc_err_3" tests when errors threshold is implemented
        Ok(())
    }

    fn on_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError> {
        self.processor
            .commit(epoch)
            .map_err(ExecutionError::ProcessorOrSink)?;
        self.channel_manager.store_and_send_commit(epoch)
    }

    fn on_terminate(&mut self) -> Result<(), ExecutionError> {
        self.channel_manager.send_terminate()
    }

    fn on_snapshotting_done(&mut self, connection_name: String) -> Result<(), ExecutionError> {
        self.channel_manager.send_snapshotting_done(connection_name)
    }
}
