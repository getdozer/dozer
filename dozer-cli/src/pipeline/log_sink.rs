use std::{collections::HashMap, fmt::Debug, sync::Arc};

use dozer_cache::dozer_log::{
    attach_progress,
    replication::{Log, LogOperation},
};
use dozer_core::{
    epoch::Epoch,
    executor_operation::ProcessorOperation,
    node::{PortHandle, Sink, SinkFactory},
    processor_record::ProcessorRecordStore,
    DEFAULT_PORT_HANDLE,
};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::errors::internal::BoxedError;
use dozer_types::indicatif::{MultiProgress, ProgressBar};
use dozer_types::types::Schema;
use tokio::{runtime::Runtime, sync::Mutex};

#[derive(Debug)]
pub struct LogSinkFactory {
    runtime: Arc<Runtime>,
    log: Arc<Mutex<Log>>,
    endpoint_name: String,
    multi_pb: MultiProgress,
}

impl LogSinkFactory {
    pub fn new(
        runtime: Arc<Runtime>,
        log: Arc<Mutex<Log>>,
        endpoint_name: String,
        multi_pb: MultiProgress,
    ) -> Self {
        Self {
            runtime,
            log,
            endpoint_name,
            multi_pb,
        }
    }
}

impl SinkFactory<SchemaSQLContext> for LogSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn prepare(
        &self,
        input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), BoxedError> {
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
            Some(self.multi_pb.clone()),
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
        multi_pb: Option<MultiProgress>,
    ) -> Self {
        let pb = attach_progress(multi_pb);
        pb.set_message(endpoint_name);
        Self {
            runtime,
            log,
            pb,
            counter: 0,
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
        self.runtime.block_on(async {
            let mut log = self.log.lock().await;
            log.write(
                dozer_cache::dozer_log::replication::LogOperation::Op {
                    op: record_store
                        .load_operation(&op)
                        .map_err(Into::<BoxedError>::into)?,
                },
                self.log.clone(),
            )
            .await
            .map_err(Into::<BoxedError>::into)
        })?;
        self.update_counter();
        Ok(())
    }

    fn commit(&mut self, epoch_details: &Epoch) -> Result<(), BoxedError> {
        self.runtime.block_on(async {
            let mut log = self.log.lock().await;
            log.write(
                LogOperation::Commit {
                    epoch: epoch_details.clone(),
                },
                self.log.clone(),
            )
            .await
        })?;
        self.update_counter();
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self, connection_name: String) -> Result<(), BoxedError> {
        self.runtime.block_on(async {
            let mut log = self.log.lock().await;
            log.write(
                LogOperation::SnapshottingDone { connection_name },
                self.log.clone(),
            )
            .await
        })?;
        self.update_counter();
        Ok(())
    }
}
