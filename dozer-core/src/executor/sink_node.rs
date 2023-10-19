use std::{borrow::Cow, mem::swap, sync::Arc};

use crossbeam::channel::Receiver;
use daggy::NodeIndex;
use dozer_tracing::LabelsAndProgress;
use dozer_types::node::NodeHandle;
use metrics::{describe_counter, describe_histogram, histogram, increment_counter};

use crate::{
    builder_dag::NodeKind,
    epoch::{Epoch, EpochManager},
    error_manager::ErrorManager,
    errors::ExecutionError,
    executor_operation::{ExecutorOperation, ProcessorOperation},
    node::{PortHandle, Sink},
};

use super::execution_dag::ExecutionDag;
use super::{name::Name, receiver_loop::ReceiverLoop};

/// A sink in the execution DAG.
#[derive(Debug)]
pub struct SinkNode {
    /// Node handle in description DAG.
    node_handle: NodeHandle,
    /// The epoch id the sink was constructed for.
    initial_epoch_id: u64,
    /// Input port handles.
    port_handles: Vec<PortHandle>,
    /// Input data channels.
    receivers: Vec<Receiver<ExecutorOperation>>,
    /// The sink.
    sink: Box<dyn Sink>,
    /// Where all the records from ingested data are stored.
    epoch_manager: Arc<EpochManager>,
    /// The error manager, for reporting non-fatal errors.
    error_manager: Arc<ErrorManager>,
    /// The metrics labels.
    labels: LabelsAndProgress,
}

const SINK_OPERATION_COUNTER_NAME: &str = "sink_operation";
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

        describe_counter!(
            SINK_OPERATION_COUNTER_NAME,
            "Number of operation processed by the sink"
        );
        describe_histogram!(
            PIPELINE_LATENCY_HISTOGRAM_NAME,
            "The pipeline processing latency in seconds"
        );

        Self {
            node_handle,
            initial_epoch_id: dag.epoch_manager().epoch_id(),
            port_handles,
            receivers,
            sink,
            epoch_manager: dag.epoch_manager().clone(),
            error_manager: dag.error_manager().clone(),
            labels: dag.labels().clone(),
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

    fn on_op(&mut self, index: usize, op: ProcessorOperation) -> Result<(), ExecutionError> {
        let mut labels = self.labels.labels().clone();
        labels.push("table", self.node_handle.id.clone());
        const OPERATION_TYPE_LABEL: &str = "operation_type";
        match &op {
            ProcessorOperation::Insert { .. } => {
                labels.push(OPERATION_TYPE_LABEL, "insert");
            }
            ProcessorOperation::Delete { .. } => {
                labels.push(OPERATION_TYPE_LABEL, "delete");
            }
            ProcessorOperation::Update { .. } => {
                labels.push(OPERATION_TYPE_LABEL, "update");
            }
        }

        if let Err(e) = self.sink.process(
            self.port_handles[index],
            self.epoch_manager.record_store(),
            op,
        ) {
            self.error_manager.report(e);
        }

        increment_counter!(SINK_OPERATION_COUNTER_NAME, labels);

        Ok(())
    }

    fn on_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError> {
        // debug!("[{}] Checkpointing - {}", self.node_handle, epoch);
        if let Err(e) = self.sink.commit(epoch) {
            self.error_manager.report(e);
        }

        if let Ok(duration) = epoch.decision_instant.elapsed() {
            let mut labels = self.labels.labels().clone();
            labels.push("endpoint", self.node_handle.id.clone());
            histogram!(PIPELINE_LATENCY_HISTOGRAM_NAME, duration, labels);
        }

        if let Some(queue) = epoch.common_info.sink_persist_queue.as_ref() {
            if let Err(e) = self.sink.persist(queue) {
                self.error_manager.report(e);
            }
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
