use crossbeam::channel::{Receiver, Sender, TryRecvError};
use daggy::NodeIndex;
use dozer_tracing::{
    constants::{
        ConnectorEntityType, DOZER_METER_NAME, OPERATION_TYPE_LABEL, PIPELINE_LATENCY_GAUGE_NAME,
        SINK_OPERATION_COUNTER_NAME, TABLE_LABEL, TOTAL_LATENCY_HISTOGRAM_NAME,
    },
    emit_event,
    opentelemetry_metrics::{Counter, Gauge, Histogram},
    DozerMonitorContext,
};
use dozer_types::{epoch::SourceTime, log::warn};
use dozer_types::{
    log::debug,
    node::{NodeHandle, OpIdentifier},
    tracing::error,
    types::{Operation, TableOperation},
};
use std::{
    borrow::Cow,
    mem::swap,
    sync::Arc,
    time::{Duration, Instant},
    usize,
};
use tokio::sync::broadcast;

use crate::{
    builder_dag::NodeKind, epoch::Epoch, error_manager::ErrorManager, errors::ExecutionError,
    event::Event, executor_operation::ExecutorOperation, node::Sink,
};

use super::execution_dag::ExecutionDag;
use super::{name::Name, receiver_loop::ReceiverLoop};

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(20);

struct FlushScheduler {
    receiver: Receiver<Duration>,
    sender: Sender<()>,
    next_schedule: Option<Duration>,
    next_schedule_from: Instant,
    loop_interval: Duration,
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

            let elapsed = self.next_schedule_from.elapsed();
            if elapsed >= schedule {
                let Ok(_) = self.sender.send(()) else {
                    return;
                };
                self.next_schedule = None;
            } else {
                let time_to_next_schedule = schedule - elapsed;
                std::thread::sleep(self.loop_interval.min(time_to_next_schedule));
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
    labels: DozerMonitorContext,

    max_flush_interval: Duration,

    ops_since_flush: u64,
    last_op_if_commit: Option<Epoch>,
    flush_scheduled_on_next_commit: bool,
    flush_scheduler_sender: Sender<Duration>,
    should_flush_receiver: Receiver<()>,

    event_sender: broadcast::Sender<Event>,
    metrics: SinkMetrics,
    source_times: Option<Vec<SourceTime>>,
}

#[derive(Debug)]

pub struct SinkMetrics {
    sink_counter: Counter<u64>,
    latency_gauge: Gauge<f64>,
    total_latency_hist: Histogram<u64>,
}

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

        let meter = dozer_tracing::global::meter(DOZER_METER_NAME);
        let sink_counter = meter
            .u64_counter(SINK_OPERATION_COUNTER_NAME)
            .with_description("No of operations in the sink node")
            .init();
        let latency_gauge = meter
            .f64_gauge(PIPELINE_LATENCY_GAUGE_NAME)
            .with_description("Mesasures latency between commits")
            .init();

        let total_latency_hist = meter
            .u64_histogram(TOTAL_LATENCY_HISTOGRAM_NAME)
            .with_description("Measures total latency between commit on source and commit on sink")
            .init();

        let max_flush_interval = sink
            .max_batch_duration_ms()
            .map_or(DEFAULT_FLUSH_INTERVAL, Duration::from_millis);
        let (schedule_sender, schedule_receiver) = crossbeam::channel::bounded(10);
        let (should_flush_sender, should_flush_receiver) = crossbeam::channel::bounded(0);
        let mut scheduler = FlushScheduler {
            receiver: schedule_receiver,
            sender: should_flush_sender,
            next_schedule: None,
            next_schedule_from: Instant::now(),
            loop_interval: max_flush_interval / 5,
        };

        std::thread::spawn(move || scheduler.run());
        let source_times = sink.supports_batching().then(Vec::new);

        Self {
            node_handle,
            initial_epoch_id: dag.initial_epoch_id(),
            node_handles,
            receivers,
            sink,
            error_manager: dag.error_manager().clone(),
            labels: dag.labels().clone(),
            last_op_if_commit: None,
            flush_scheduled_on_next_commit: false,
            flush_scheduler_sender: schedule_sender,
            should_flush_receiver,
            event_sender: dag.event_hub().sender.clone(),
            max_flush_interval,
            ops_since_flush: 0,
            source_times,
            metrics: SinkMetrics {
                sink_counter,
                latency_gauge,
                total_latency_hist,
            },
        }
    }

    pub fn handle(&self) -> &NodeHandle {
        &self.node_handle
    }

    fn flush(&mut self, epoch: Epoch) -> Result<(), ExecutionError> {
        if let Err(e) = self.sink.flush_batch() {
            self.error_manager.report(e);
        }
        self.ops_since_flush = 0;
        self.flush_scheduler_sender
            .send(self.max_flush_interval)
            .unwrap();
        let _ = self.event_sender.send(Event::SinkFlushed {
            node: self.node_handle.clone(),
            epoch,
        });
        if let Some(source_times) = self.source_times.as_mut() {
            let mut labels = self.labels.attrs().clone();
            labels.push(dozer_tracing::KeyValue::new(
                TABLE_LABEL,
                self.node_handle.id.clone(),
            ));
            for time in source_times.drain(..) {
                if let Some(elapsed) = time.elapsed_millis() {
                    self.metrics.total_latency_hist.record(elapsed, &labels);
                }
            }
        }
        Ok(())
    }
}

impl Name for SinkNode {
    fn name(&self) -> Cow<str> {
        Cow::Owned(self.node_handle.to_string())
    }
}

struct Select<'a> {
    op_receivers: &'a [Receiver<ExecutorOperation>],
    flush_receiver: &'a Receiver<()>,
    inner: crossbeam::channel::Select<'a>,
    flush_idx: usize,
}

enum ReceiverMsg {
    Op(usize, ExecutorOperation),
    Flush,
}

impl<'a> Select<'a> {
    fn new(
        op_receivers: &'a [Receiver<ExecutorOperation>],
        flush_receiver: &'a Receiver<()>,
    ) -> Self {
        let mut inner = crossbeam::channel::Select::new();
        for recv in op_receivers {
            let _ = inner.recv(recv);
        }
        let flush_idx = inner.recv(flush_receiver);
        Self {
            inner,
            flush_idx,
            op_receivers,
            flush_receiver,
        }
    }

    fn remove(&mut self, idx: usize) {
        self.inner.remove(idx);
    }

    fn reinit(&mut self) {
        self.inner = crossbeam::channel::Select::new();
        for recv in self.op_receivers {
            let _ = self.inner.recv(recv);
        }
        self.flush_idx = self.inner.recv(self.flush_receiver);
    }

    fn recv(&mut self) -> Result<ReceiverMsg, ExecutionError> {
        let msg = self.inner.select();
        let index = msg.index();
        let res = if index == self.flush_idx {
            msg.recv(self.flush_receiver).map(|_| ReceiverMsg::Flush)
        } else {
            msg.recv(&self.op_receivers[index])
                .map(|op| ReceiverMsg::Op(index, op))
        };
        res.map_err(|_| ExecutionError::CannotReceiveFromChannel)
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

    fn receiver_loop(mut self, initial_epoch_id: u64) -> Result<(), ExecutionError> {
        // This is just copied from ReceiverLoop
        let receivers = self.receivers();
        let should_flush_receiver = {
            // Take the receiver. This is fine, as long as we exclusively use the
            // returned receiver and not the one in `self`.
            let (_, mut tmp_recv) = crossbeam::channel::bounded(0);
            swap(&mut self.should_flush_receiver, &mut tmp_recv);
            tmp_recv
        };
        debug_assert!(
            !receivers.is_empty(),
            "Processor or sink must have at least 1 incoming edge"
        );
        let mut is_terminated = vec![false; receivers.len()];

        let mut commits_received: usize = 0;
        let mut epoch_id = initial_epoch_id;

        self.flush_scheduler_sender
            .send(self.max_flush_interval)
            .unwrap();
        let mut sel = Select::new(&receivers, &should_flush_receiver);
        loop {
            let ReceiverMsg::Op(index, op) = sel.recv()? else {
                if let Some(epoch) = self.last_op_if_commit.take() {
                    self.flush(epoch)?;
                } else {
                    self.flush_scheduled_on_next_commit = true;
                }
                continue;
            };

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
                        sel.reinit();
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
                    emit_event(
                        &connection_name,
                        &ConnectorEntityType::Connector,
                        &self.labels,
                        "snapshotting_started",
                    );
                    self.on_snapshotting_started(connection_name)?;
                }
                ExecutorOperation::SnapshottingDone {
                    connection_name,
                    id,
                } => {
                    emit_event(
                        &connection_name,
                        &ConnectorEntityType::Connector,
                        &self.labels,
                        "snapshotting_done",
                    );
                    self.on_snapshotting_done(connection_name, id)?;
                }
            }
        }
    }

    fn on_op(&mut self, _index: usize, op: TableOperation) -> Result<(), ExecutionError> {
        self.last_op_if_commit = None;
        let mut labels = self.labels.attrs();

        labels.push(dozer_tracing::KeyValue::new(
            TABLE_LABEL,
            self.node_handle.id.clone(),
        ));

        let op_str = match &op.op {
            Operation::Insert { .. } => "insert",
            Operation::Delete { .. } => "delete",
            Operation::Update { .. } => "update",
            Operation::BatchInsert { .. } => "insert",
        };
        labels.push(dozer_tracing::KeyValue::new(OPERATION_TYPE_LABEL, op_str));
        let counter_number: u64 = match &op.op {
            Operation::BatchInsert { new } => new.len() as u64,
            _ => 1,
        };
        self.ops_since_flush += counter_number;

        if let Err(e) = self.sink.process(op) {
            self.error_manager.report(e);
        }

        self.metrics.sink_counter.add(counter_number, &labels);
        Ok(())
    }

    fn on_commit(&mut self, epoch: Epoch) -> Result<(), ExecutionError> {
        // debug!("[{}] Checkpointing - {}", self.node_handle, epoch);
        if let Err(e) = self.sink.commit(&epoch) {
            self.error_manager.report(e);
        }
        self.last_op_if_commit = Some(epoch.clone());

        match epoch.decision_instant.elapsed() {
            Ok(duration) => {
                let mut labels = self.labels.attrs().clone();
                labels.push(dozer_tracing::KeyValue::new(
                    TABLE_LABEL,
                    self.node_handle.id.clone(),
                ));
                self.metrics
                    .latency_gauge
                    .record(duration.as_secs_f64(), &labels);
            }
            Err(e) => {
                error!("error recording pipeline_latench {:?}", e);
            }
        }

        if let Some(source_time) = epoch.source_time {
            if let Some(source_times) = self.source_times.as_mut() {
                source_times.push(source_time);
            } else {
                let mut labels = self.labels.attrs().clone();
                labels.push(dozer_tracing::KeyValue::new(
                    TABLE_LABEL,
                    self.node_handle.id.clone(),
                ));
                if let Some(elapsed) = source_time.elapsed_millis() {
                    self.metrics.total_latency_hist.record(elapsed, &labels);
                } else {
                    warn!("Recorded total latency < 0. Source clock and system clock are out of sync.");
                }
            }
        }

        if self
            .sink
            .preferred_batch_size()
            .is_some_and(|batch_size| self.ops_since_flush >= batch_size)
            || self.flush_scheduled_on_next_commit
        {
            self.flush(epoch)?;
            self.flush_scheduled_on_next_commit = false;
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

        // record 0 as pipeline latency when snapshotting is done
        // this is to initialize the value for metrics
        let mut labels = self.labels.attrs().clone();
        labels.push(dozer_tracing::KeyValue::new(
            TABLE_LABEL,
            self.node_handle.id.clone(),
        ));
        self.metrics.latency_gauge.record(0_f64, &labels);

        Ok(())
    }
}
