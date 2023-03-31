use std::{collections::HashMap, io::Write, path::PathBuf};

use dozer_core::{
    epoch::Epoch,
    errors::ExecutionError,
    executor::ExecutorOperation,
    node::{PortHandle, Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::{
    bytes::{BufMut, BytesMut},
    indicatif::MultiProgress,
    models::api_endpoint::ApiEndpoint,
    types::{Operation, Schema},
};
use std::fs::OpenOptions;

use crate::utils::get_endpoint_log_path;

#[derive(Debug, Clone)]
pub struct LogSinkSettings {
    pub pipeline_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LogSinkFactory {
    settings: LogSinkSettings,
    api_endpoint: ApiEndpoint,
    multi_pb: MultiProgress,
}

impl LogSinkFactory {
    pub fn new(
        settings: LogSinkSettings,
        api_endpoint: ApiEndpoint,
        multi_pb: MultiProgress,
    ) -> Self {
        Self {
            settings,
            api_endpoint,
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
    ) -> Result<(), ExecutionError> {
        debug_assert!(input_schemas.len() == 1);
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, ExecutionError> {
        let log_path = get_endpoint_log_path(&self.settings.pipeline_dir, &self.api_endpoint.name);

        std::fs::create_dir_all(log_path.as_path())
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        Ok(Box::new(LogSink::new(
            Some(self.multi_pb.clone()),
            log_path,
        )?))
    }
}

#[derive(Debug)]
pub struct LogSink {
    multi_pb: Option<MultiProgress>,
    file: std::fs::File,
}

impl LogSink {
    pub fn new(multi_pb: Option<MultiProgress>, log_path: PathBuf) -> Result<Self, ExecutionError> {
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(log_path)
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
        Ok(Self { multi_pb, file })
    }
}

impl Sink for LogSink {
    fn process(&mut self, _from_port: PortHandle, op: Operation) -> Result<(), ExecutionError> {
        let executor_operation = ExecutorOperation::Op { op };
        let msg = dozer_types::bincode::serialize(&executor_operation)
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        write_msg_to_file(&mut self.file, &msg)
    }

    fn commit(&mut self) -> Result<(), ExecutionError> {
        let executor_operation = ExecutorOperation::Commit {
            epoch: Epoch::new(0, Default::default()),
        };
        let msg = dozer_types::bincode::serialize(&executor_operation)
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
        write_msg_to_file(&mut self.file, &msg)
    }

    fn on_source_snapshotting_done(&mut self) -> Result<(), ExecutionError> {
        let executor_operation = ExecutorOperation::SnapshottingDone {};
        todo!("Write snapshotting done to log file.")
    }
}

fn write_msg_to_file(file: &mut std::fs::File, msg: &[u8]) -> Result<(), ExecutionError> {
    let mut buf = BytesMut::with_capacity(msg.len() + 4);
    buf.put_u32_le(msg.len() as u32);
    buf.put_slice(msg);
    file.write_all(&buf)
        .map_err(|e| ExecutionError::InternalError(Box::new(e)))
}
