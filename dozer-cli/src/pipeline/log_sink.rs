use std::{collections::HashMap, fmt::Debug, ops::Deref, sync::Arc};

use dozer_cache::dozer_log::{
    replication::{Log, LogOperation},
    storage::Queue,
};
use dozer_core::{
    epoch::Epoch,
    node::{PortHandle, Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_recordstore::ProcessorRecordStore;
use dozer_tracing::LabelsAndProgress;
use dozer_types::indicatif::ProgressBar;
use dozer_types::types::Schema;
use dozer_types::{errors::internal::BoxedError, types::Operation};
use tokio::{runtime::Runtime, sync::Mutex};

#[derive(Debug)]
pub struct LogSinkFactory {
    runtime: Arc<Runtime>,
    log: Arc<Mutex<Log>>,
    endpoint_name: String,
    labels: LabelsAndProgress,
}

impl LogSinkFactory {
    pub fn new(
        runtime: Arc<Runtime>,
        log: Arc<Mutex<Log>>,
        endpoint_name: String,
        labels: LabelsAndProgress,
    ) -> Self {
        Self {
            runtime,
            log,
            endpoint_name,
            labels,
        }
    }
}

impl SinkFactory for LogSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn prepare(&self, input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        debug_assert!(input_schemas.len() == 1);
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        Ok(Box::new(LogSink::new(
            self.runtime.clone(),
            self.log.clone(),
            self.endpoint_name.clone(),
            self.labels.clone(),
        )))
    }
}

#[derive(Debug)]
pub struct LogSink {
    runtime: Arc<Runtime>,
    log: Arc<Mutex<Log>>,
    pb: ProgressBar,
}

impl LogSink {
    pub fn new(
        runtime: Arc<Runtime>,
        log: Arc<Mutex<Log>>,
        endpoint_name: String,
        labels: LabelsAndProgress,
    ) -> Self {
        let pb = labels.create_progress_bar(endpoint_name);
        Self { runtime, log, pb }
    }
}

impl Sink for LogSink {
    fn process(
        &mut self,
        _from_port: PortHandle,
        _record_store: &ProcessorRecordStore,
        op: Operation,
    ) -> Result<(), BoxedError> {
        let end = self
            .runtime
            .block_on(self.log.lock())
            .write(dozer_cache::dozer_log::replication::LogOperation::Op { op });
        self.pb.set_position(end as u64);
        Ok(())
    }

    fn commit(&mut self, epoch_details: &Epoch) -> Result<(), BoxedError> {
        let end = self
            .runtime
            .block_on(self.log.lock())
            .write(LogOperation::Commit {
                source_states: epoch_details.common_info.source_states.deref().clone(),
                decision_instant: epoch_details.decision_instant,
            });
        self.pb.set_position(end as u64);
        Ok(())
    }

    fn persist(&mut self, epoch: &Epoch, queue: &Queue) -> Result<(), BoxedError> {
        self.runtime.block_on(self.log.lock()).persist(
            epoch.common_info.id,
            queue,
            self.log.clone(),
            &self.runtime,
        )?;
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self, connection_name: String) -> Result<(), BoxedError> {
        let end = self
            .runtime
            .block_on(self.log.lock())
            .write(LogOperation::SnapshottingDone { connection_name });
        self.pb.set_position(end as u64);
        Ok(())
    }
}
