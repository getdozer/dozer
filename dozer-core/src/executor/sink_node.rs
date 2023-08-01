use std::{borrow::Cow, collections::HashMap, mem::swap, sync::Arc};

use crossbeam::channel::Receiver;
use daggy::NodeIndex;
use dozer_types::{epoch::Epoch, node::NodeHandle};
use metrics::{describe_histogram, histogram};

use crate::{
    builder_dag::NodeKind,
    error_manager::ErrorManager,
    errors::ExecutionError,
    executor_operation::{ExecutorOperation, ProcessorOperation},
    forwarder::StateWriter,
    node::{PortHandle, Sink},
};

use super::execution_dag::ExecutionDag;
use super::{name::Name, receiver_loop::ReceiverLoop};

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
    /// This node's state writer, for writing metadata and port state.
    state_writer: StateWriter,
    /// The error manager, for reporting non-fatal errors.
    error_manager: Arc<ErrorManager>,
}

const PIPELINE_LATENCY_HISTOGRAM_NAME: &str = "pipeline_latency";

impl SinkNode {
    pub fn new(dag: &mut ExecutionDag, node_index: NodeIndex) -> Self {
        let Some(node) = dag.node_weight_mut(node_index).take() else {
            panic!("Must pass in a node")
        };
        let node_handle = node.handle;
        let NodeKind::Sink(sink) = node.kind else {
            panic!("Must pass in a sink node");
        };

        let (port_handles, receivers) = dag.collect_receivers(node_index);

        let state_writer = StateWriter::new(HashMap::new());

        describe_histogram!(
            PIPELINE_LATENCY_HISTOGRAM_NAME,
            "The pipeline processing latency in seconds"
        );

        Self {
            node_handle,
            port_handles,
            receivers,
            sink,
            state_writer,
            error_manager: dag.error_manager().clone(),
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

    fn on_op(&mut self, index: usize, op: ProcessorOperation) -> Result<(), ExecutionError> {
        if let Err(e) = self.sink.process(self.port_handles[index], op) {
            self.error_manager.report(e);
        }
        Ok(())
    }

    fn on_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError> {
        // debug!("[{}] Checkpointing - {}", self.node_handle, epoch);
        if let Err(e) = self.sink.commit(epoch) {
            self.error_manager.report(e);
        }
        self.state_writer.store_commit_info(epoch)?;

        if let Ok(duration) = epoch.decision_instant.elapsed() {
            histogram!(PIPELINE_LATENCY_HISTOGRAM_NAME, duration, "endpoint" => self.node_handle.id.clone());
        }

        Ok(())
    }

    fn on_terminate(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn on_snapshotting_done(&mut self, connection_name: String) -> Result<(), ExecutionError> {
        if let Err(e) = self.sink.on_source_snapshotting_done(connection_name) {
            self.error_manager.report(e);
        }
        Ok(())
    }
}
