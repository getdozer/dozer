use std::{collections::HashMap, path::PathBuf};

use dozer_cache::dozer_log::{errors::WriterError, writer::LogWriter};
use dozer_core::{
    epoch::Epoch,
    node::{PortHandle, Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::indicatif::MultiProgress;
use dozer_types::types::{Operation, Schema};
use dozer_types::{epoch::ExecutorOperation, errors::internal::BoxedError};

#[derive(Debug, Clone)]
pub struct LogSinkSettings {
    pub file_buffer_capacity: usize,
}

#[derive(Debug, Clone)]
pub struct LogSinkFactory {
    log_path: PathBuf,
    settings: LogSinkSettings,
    endpoint_name: String,
    multi_pb: MultiProgress,
}

impl LogSinkFactory {
    pub fn new(
        log_path: PathBuf,
        settings: LogSinkSettings,
        endpoint_name: String,
        multi_pb: MultiProgress,
    ) -> Self {
        Self {
            log_path,
            settings,
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
            Some(self.multi_pb.clone()),
            self.log_path.clone(),
            self.settings.file_buffer_capacity,
            self.endpoint_name.clone(),
        )?))
    }
}

#[derive(Debug)]
pub struct LogSink(LogWriter);

impl LogSink {
    pub fn new(
        multi_pb: Option<MultiProgress>,
        log_path: PathBuf,
        file_buffer_capacity: usize,
        endpoint_name: String,
    ) -> Result<Self, WriterError> {
        Ok(Self(LogWriter::new(
            log_path,
            file_buffer_capacity,
            endpoint_name,
            multi_pb,
        )?))
    }
}

impl Sink for LogSink {
    fn process(&mut self, _from_port: PortHandle, op: Operation) -> Result<(), BoxedError> {
        self.0
            .write_op(&ExecutorOperation::Op { op })
            .map_err(Into::into)
    }

    fn commit(&mut self) -> Result<(), BoxedError> {
        self.0.write_op(&ExecutorOperation::Commit {
            epoch: Epoch::new(0, Default::default()),
        })?;
        self.0.flush()?;
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self, connection_name: String) -> Result<(), BoxedError> {
        self.0
            .write_op(&ExecutorOperation::SnapshottingDone { connection_name })
            .map_err(Into::into)
    }
}
