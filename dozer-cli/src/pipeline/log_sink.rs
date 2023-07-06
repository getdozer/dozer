use std::{collections::HashMap, fmt::Debug, sync::Arc};

use dozer_cache::dozer_log::{attach_progress, replication::Log, storage::Storage};
use dozer_core::{
    epoch::Epoch,
    node::{PortHandle, Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::indicatif::{MultiProgress, ProgressBar};
use dozer_types::types::{Operation, Schema};
use dozer_types::{epoch::ExecutorOperation, errors::internal::BoxedError};
use tokio::{runtime::Runtime, sync::Mutex};

#[derive(Debug)]
pub struct LogSinkFactory<S: Storage> {
    runtime: Arc<Runtime>,
    log: Arc<Mutex<Log<S>>>,
    endpoint_name: String,
    multi_pb: MultiProgress,
}

impl<S: Storage> LogSinkFactory<S> {
    pub fn new(
        runtime: Arc<Runtime>,
        log: Arc<Mutex<Log<S>>>,
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

impl<S: Storage + Debug> SinkFactory<SchemaSQLContext> for LogSinkFactory<S> {
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
pub struct LogSink<S: Storage> {
    runtime: Arc<Runtime>,
    log: Arc<Mutex<Log<S>>>,
    pb: ProgressBar,
    counter: u64,
}

impl<S: Storage> LogSink<S> {
    pub fn new(
        runtime: Arc<Runtime>,
        log: Arc<Mutex<Log<S>>>,
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

impl<S: Storage + Debug> Sink for LogSink<S> {
    fn process(&mut self, _from_port: PortHandle, op: Operation) -> Result<(), BoxedError> {
        self.runtime.block_on(async {
            let mut log = self.log.lock().await;
            log.write(ExecutorOperation::Op { op }, self.log.clone());
        });
        self.update_counter();
        Ok(())
    }

    fn commit(&mut self, epoch_details: &Epoch) -> Result<(), BoxedError> {
        self.runtime.block_on(async {
            let mut log = self.log.lock().await;
            log.write(
                ExecutorOperation::Commit {
                    epoch: epoch_details.clone(),
                },
                self.log.clone(),
            );
        });
        self.update_counter();
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self, connection_name: String) -> Result<(), BoxedError> {
        self.runtime.block_on(async {
            let mut log = self.log.lock().await;
            log.write(
                ExecutorOperation::SnapshottingDone { connection_name },
                self.log.clone(),
            );
        });
        self.update_counter();
        Ok(())
    }
}
