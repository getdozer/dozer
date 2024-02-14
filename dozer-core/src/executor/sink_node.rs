use crossbeam::channel::{Receiver, Sender, TryRecvError};
use daggy::NodeIndex;
use dozer_tracing::LabelsAndProgress;
use dozer_types::{
    log::debug,
    node::{NodeHandle, OpIdentifier},
    types::{Operation, TableOperation},
};
use metrics::{counter, describe_counter, describe_gauge, gauge};
use std::{
    borrow::Cow,
    mem::swap,
    sync::Arc,
    time::{Duration, Instant},
    usize,
};

use crate::{
    builder_dag::NodeKind, epoch::Epoch, error_manager::ErrorManager, errors::ExecutionError,
    executor::receiver_loop::init_select, executor_operation::ExecutorOperation, node::Sink,
};

use super::execution_dag::ExecutionDag;
use super::{name::Name, receiver_loop::ReceiverLoop};

// TODO: make configurable
const SCHEDULE_LOOP_INTERVAL: Duration = Duration::from_millis(5);
const MAX_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

struct FlushScheduler {
    receiver: Receiver<Duration>,
    sender: Sender<()>,
    next_schedule: Option<Duration>,
    next_schedule_from: Instant,
}

impl FlushScheduler {
    fn run(&mut self) {
        loop {
            // If we have nothing scheduled, block until we get a schedule
            let mut next_schedule = if self.next_schedule.is_none() {
                match self.receiver.recv() {
                    Ok(v) => Some(v),
                    Err(_) => return,
                }
            } else {
                None
            };

            // Keep postponing the schedule while there are messages
            while let Some(sched) = match self.receiver.try_recv() {
                Ok(next) => Some(next),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Disconnected) => return,
            } {
                next_schedule = Some(sched);
            }

            if let Some(next) = next_schedule {
                self.next_schedule = Some(next);
                self.next_schedule_from = Instant::now();
            }

            let Some(schedule) = self.next_schedule else {
                continue;
            };

            if self.next_schedule_from.elapsed() > schedule {
                let Ok(_) = self.sender.send(()) else {
                    return;
                };
                self.next_schedule = None;
            } else {
                let time_to_next_schedule = schedule - self.next_schedule_from.elapsed();
                std::thread::sleep(SCHEDULE_LOOP_INTERVAL.min(time_to_next_schedule));
            }
        }
    }
}

/// A sink in the execution DAG.
#[derive(Debug)]
pub struct SinkNode {
    /// Node handle in description DAG.
    node_handle: NodeHandle,
    /// The epoch id the sink was constructed for.
    initial_epoch_id: u64,
    /// Input node handles.
    node_handles: Vec<NodeHandle>,
    /// Input data channels.
    receivers: Vec<Receiver<ExecutorOperation>>,
    /// The sink.
    sink: Box<dyn Sink>,
    /// The error manager, for reporting non-fatal errors.
    error_manager: Arc<ErrorManager>,
    /// The metrics labels.
    labels: LabelsAndProgress,

    last_op_was_commit: bool,
    flush_on_next_commit: bool,
    flush_scheduler_sender: Sender<Duration>,
    should_flush_receiver: Receiver<()>,
}

const SINK_OPERATION_COUNTER_NAME: &str = "sink_operation";
const PIPELINE_LATENCY_GAUGE_NAME: &str = "pipeline_latency";

impl SinkNode {
    pub fn new(dag: &mut ExecutionDag, node_index: NodeIndex) -> Self {
        let node = dag.node_weight_mut(node_index);
        let Some(kind) = node.kind.take() else {
            panic!("Must pass in a node")
        };
        let node_handle = node.handle.clone();
        let NodeKind::Sink(sink) = kind else {
            panic!("Must pass in a sink node");
        };

        let (node_handles, receivers) = dag.collect_receivers(node_index);

        describe_counter!(
            SINK_OPERATION_COUNTER_NAME,
            "Number of operation processed by the sink"
        );
        describe_gauge!(
            PIPELINE_LATENCY_GAUGE_NAME,
            "The pipeline processing latency in seconds"
        );

        let (schedule_sender, schedule_receiver) = crossbeam::channel::bounded(10);
        let (should_flush_sender, should_flush_receiver) = crossbeam::channel::bounded(0);
        let mut scheduler = FlushScheduler {
            receiver: schedule_receiver,
            sender: should_flush_sender,
            next_schedule: None,
            next_schedule_from: Instant::now(),
        };

        std::thread::spawn(move || scheduler.run());

        Self {
            node_handle,
            initial_epoch_id: dag.initial_epoch_id(),
            node_handles,
            receivers,
            sink,
            error_manager: dag.error_manager().clone(),
            labels: dag.labels().clone(),
            last_op_was_commit: false,
            flush_on_next_commit: false,
            flush_scheduler_sender: schedule_sender,
            should_flush_receiver,
        }
    }

    pub fn handle(&self) -> &NodeHandle {
        &self.node_handle
    }

    fn flush(&mut self) -> Result<(), ExecutionError> {
        if let Err(e) = self.sink.flush_batch() {
            self.error_manager.report(e);
        }
        self.flush_scheduler_sender
            .send(MAX_FLUSH_INTERVAL)
            .unwrap();
        Ok(())
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
        Cow::Owned(self.node_handles[index].to_string())
    }

    fn receiver_loop(&mut self, initial_epoch_id: u64) -> Result<(), ExecutionError> {
        // This is just copied from ReceiverLoop
        let receivers = self.receivers();
        debug_assert!(
            !receivers.is_empty(),
            "Processor or sink must have at least 1 incoming edge"
        );
        let mut is_terminated = vec![false; receivers.len()];

        let mut commits_received: usize = 0;
        let mut epoch_id = initial_epoch_id;

        self.flush_scheduler_sender
            .send(MAX_FLUSH_INTERVAL)
            .unwrap();
        let mut sel = init_select(&receivers);
        loop {
            if self.should_flush_receiver.try_recv().is_ok() {
                if self.last_op_was_commit {
                    self.flush()?;
                } else {
                    self.flush_on_next_commit = true;
                }
            }
            let index = sel.ready();
            let op = receivers[index]
                .recv()
                .map_err(|_| ExecutionError::CannotReceiveFromChannel)?;

            match op {
                ExecutorOperation::Op { op } => {
                    self.on_op(index, op)?;
                }
                ExecutorOperation::Commit { epoch } => {
                    assert_eq!(epoch.common_info.id, epoch_id);
                    commits_received += 1;
                    sel.remove(index);

                    if commits_received == receivers.len() {
                        self.on_commit(epoch)?;
                        epoch_id += 1;
                        commits_received = 0;
                        sel = init_select(&receivers);
                    }
                }
                ExecutorOperation::Terminate => {
                    is_terminated[index] = true;
                    sel.remove(index);
                    debug!(
                        "[{}] Received Terminate request from {}",
                        self.name(),
                        self.receiver_name(index)
                    );
                    if is_terminated.iter().all(|value| *value) {
                        self.on_terminate()?;
                        debug!("[{}] Quit", self.name());
                        return Ok(());
                    }
                }
                ExecutorOperation::SnapshottingStarted { connection_name } => {
                    self.on_snapshotting_started(connection_name)?;
                }
                ExecutorOperation::SnapshottingDone {
                    connection_name,
                    id,
                } => {
                    self.on_snapshotting_done(connection_name, id)?;
                }
            }
        }
    }

    fn on_op(&mut self, _index: usize, op: TableOperation) -> Result<(), ExecutionError> {
        self.last_op_was_commit = false;
        let mut labels = self.labels.labels().clone();
        labels.push("table", self.node_handle.id.clone());
        const OPERATION_TYPE_LABEL: &str = "operation_type";
        match &op.op {
            Operation::Insert { .. } => {
                labels.push(OPERATION_TYPE_LABEL, "insert");
            }
            Operation::Delete { .. } => {
                labels.push(OPERATION_TYPE_LABEL, "delete");
            }
            Operation::Update { .. } => {
                labels.push(OPERATION_TYPE_LABEL, "update");
            }
            Operation::BatchInsert { .. } => {
                labels.push(OPERATION_TYPE_LABEL, "insert");
            }
        }

        let counter_number: u64 = match &op.op {
            Operation::BatchInsert { new } => new.len() as u64,
            _ => 1,
        };

        if let Err(e) = self.sink.process(op) {
            self.error_manager.report(e);
        }

        counter!(SINK_OPERATION_COUNTER_NAME, counter_number, labels);
        Ok(())
    }

    fn on_commit(&mut self, epoch: Epoch) -> Result<(), ExecutionError> {
        // debug!("[{}] Checkpointing - {}", self.node_handle, epoch);
        if let Err(e) = self.sink.commit(&epoch) {
            self.error_manager.report(e);
        }
        self.last_op_was_commit = true;

        if let Ok(duration) = epoch.decision_instant.elapsed() {
            let mut labels = self.labels.labels().clone();
            labels.push("endpoint", self.node_handle.id.clone());
            gauge!(PIPELINE_LATENCY_GAUGE_NAME, duration.as_secs_f64(), labels);
        }

        if let Some(queue) = epoch.common_info.sink_persist_queue.as_ref() {
            if let Err(e) = self.sink.persist(&epoch, queue) {
                self.error_manager.report(e);
            }
        }
        if self.flush_on_next_commit {
            self.flush()?;
            self.flush_on_next_commit = false;
        }
        Ok(())
    }

    fn on_terminate(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn on_snapshotting_started(&mut self, connection_name: String) -> Result<(), ExecutionError> {
        if let Err(e) = self.sink.on_source_snapshotting_started(connection_name) {
            self.error_manager.report(e);
        }
        Ok(())
    }

    fn on_snapshotting_done(
        &mut self,
        connection_name: String,
        id: Option<OpIdentifier>,
    ) -> Result<(), ExecutionError> {
        if let Err(e) = self.sink.on_source_snapshotting_done(connection_name, id) {
            self.error_manager.report(e);
        }
        Ok(())
    }
}
