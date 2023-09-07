use std::{collections::HashMap, fmt::Debug, sync::Arc};

use dozer_cache::dozer_log::{
    replication::{Log, LogOperation},
    storage::Queue,
};
use dozer_core::{
    epoch::Epoch,
    executor_operation::ProcessorOperation,
    node::{PortHandle, Sink, SinkFactory},
    processor_record::ProcessorRecordStore,
    DEFAULT_PORT_HANDLE,
};
use dozer_tracing::LabelsAndProgress;
use dozer_types::indicatif::ProgressBar;
use dozer_types::types::Schema;
use dozer_types::{errors::internal::BoxedError, parking_lot::Mutex};
use tokio::runtime::Runtime;

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
    counter: u64,
}

impl LogSink {
    pub fn new(
        runtime: Arc<Runtime>,
        log: Arc<Mutex<Log>>,
        endpoint_name: String,
        labels: LabelsAndProgress,
    ) -> Self {
        let pb = labels.create_progress_bar(endpoint_name);
        let counter = log.lock().end() as u64;
        Self {
            runtime,
            log,
            pb,
            counter,
        }
    }

    fn update_counter(&mut self) {
        self.counter += 1;
        self.pb.set_position(self.counter);
    }
}

impl Sink for LogSink {
    fn process(
        &mut self,
        _from_port: PortHandle,
        record_store: &ProcessorRecordStore,
        op: ProcessorOperation,
    ) -> Result<(), BoxedError> {
        self.log
            .lock()
            .write(dozer_cache::dozer_log::replication::LogOperation::Op {
                op: record_store
                    .load_operation(&op)
                    .map_err(Into::<BoxedError>::into)?,
            });
        self.update_counter();
        Ok(())
    }

    fn commit(&mut self, epoch_details: &Epoch) -> Result<(), BoxedError> {
        self.log.lock().write(LogOperation::Commit {
            decision_instant: epoch_details.decision_instant,
        });
        self.update_counter();
        Ok(())
    }

    fn persist(&mut self, queue: &Queue) -> Result<(), BoxedError> {
        self.log
            .lock()
            .persist(queue, self.log.clone(), &self.runtime)?;
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self, connection_name: String) -> Result<(), BoxedError> {
        self.log
            .lock()
            .write(LogOperation::SnapshottingDone { connection_name });
        self.update_counter();
        Ok(())
    }
}
