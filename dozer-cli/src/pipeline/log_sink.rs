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
use dozer_tracing::LabelsAndProgress;
use dozer_types::types::Schema;
use dozer_types::{errors::internal::BoxedError, node::OpIdentifier};
use dozer_types::{indicatif::ProgressBar, types::TableOperation};
use tokio::{runtime::Runtime, sync::Mutex};

use crate::async_trait::async_trait;

#[derive(Debug)]
pub struct LogSinkFactory {
    runtime: Arc<Runtime>,
    log: Arc<Mutex<Log>>,
    table_name: String,
    labels: LabelsAndProgress,
}

impl LogSinkFactory {
    pub fn new(
        runtime: Arc<Runtime>,
        log: Arc<Mutex<Log>>,
        table_name: String,
        labels: LabelsAndProgress,
    ) -> Self {
        Self {
            runtime,
            log,
            table_name,
            labels,
        }
    }
}

#[async_trait]
impl SinkFactory for LogSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn prepare(&self, input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        debug_assert!(input_schemas.len() == 1);
        Ok(())
    }

    async fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        Ok(Box::new(LogSink::new(
            self.runtime.clone(),
            self.log.clone(),
            self.table_name.clone(),
            self.labels.clone(),
        )))
    }

    fn type_name(&self) -> String {
        "log".to_string()
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
        table_name: String,
        labels: LabelsAndProgress,
    ) -> Self {
        let pb = labels.create_progress_bar(table_name);
        Self { runtime, log, pb }
    }
}

impl Sink for LogSink {
    fn process(&mut self, op: TableOperation) -> Result<(), BoxedError> {
        let end = self
            .runtime
            .block_on(self.log.lock())
            .write(dozer_cache::dozer_log::replication::LogOperation::Op { op: op.op });
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

    fn on_source_snapshotting_started(
        &mut self,
        connection_name: String,
    ) -> Result<(), BoxedError> {
        let end = self
            .runtime
            .block_on(self.log.lock())
            .write(LogOperation::SnapshottingStarted { connection_name });
        self.pb.set_position(end as u64);
        Ok(())
    }

    fn on_source_snapshotting_done(
        &mut self,
        connection_name: String,
        _id: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        let end = self
            .runtime
            .block_on(self.log.lock())
            .write(LogOperation::SnapshottingDone { connection_name });
        self.pb.set_position(end as u64);
        Ok(())
    }

    fn set_source_state(&mut self, _source_state: &[u8]) -> Result<(), BoxedError> {
        Ok(())
    }

    fn get_source_state(&mut self) -> Result<Option<Vec<u8>>, BoxedError> {
        Ok(None)
    }

    fn get_latest_op_id(&mut self) -> Result<Option<OpIdentifier>, BoxedError> {
        Ok(None)
    }
}
