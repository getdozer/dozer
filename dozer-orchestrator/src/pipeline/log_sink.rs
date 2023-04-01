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
    indicatif::{MultiProgress, ProgressBar, ProgressStyle},
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

        if let Some(log_dir) = log_path.as_path().parent() {
            std::fs::create_dir_all(log_dir)
                .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
        }

        Ok(Box::new(LogSink::new(
            Some(self.multi_pb.clone()),
            log_path,
            &self.api_endpoint.name,
        )?))
    }
}

#[derive(Debug)]
pub struct LogSink {
    pb: ProgressBar,
    file: std::fs::File,
    counter: usize,
}

impl LogSink {
    pub fn new(
        multi_pb: Option<MultiProgress>,
        log_path: PathBuf,
        name: &str,
    ) -> Result<Self, ExecutionError> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(log_path)
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        let pb = attach_progress(multi_pb);
        pb.set_message(name.to_string());

        Ok(Self {
            pb,
            file,
            counter: 0,
        })
    }
}

impl Sink for LogSink {
    fn process(&mut self, _from_port: PortHandle, op: Operation) -> Result<(), ExecutionError> {
        let msg = ExecutorOperation::Op { op };
        self.counter += 1;
        self.pb.set_position(self.counter as u64);
        write_msg_to_file(&mut self.file, &msg)
    }

    fn commit(&mut self) -> Result<(), ExecutionError> {
        let msg = ExecutorOperation::Commit {
            epoch: Epoch::new(0, Default::default()),
        };

        write_msg_to_file(&mut self.file, &msg)
    }

    fn on_source_snapshotting_done(&mut self) -> Result<(), ExecutionError> {
        let msg = ExecutorOperation::SnapshottingDone {};
        write_msg_to_file(&mut self.file, &msg)
    }
}

fn write_msg_to_file(
    file: &mut std::fs::File,
    msg: &ExecutorOperation,
) -> Result<(), ExecutionError> {
    let msg = dozer_types::bincode::serialize(msg)
        .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

    let mut buf = BytesMut::with_capacity(msg.len() + 4);
    buf.put_u64_le(msg.len() as u64);
    buf.put_slice(&msg);

    file.write_all(&buf)
        .map_err(|e| ExecutionError::InternalError(Box::new(e)))
}

fn attach_progress(multi_pb: Option<MultiProgress>) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    multi_pb.as_ref().map(|m| m.add(pb.clone()));
    pb.set_style(
        ProgressStyle::with_template("{spinner:.blue} {msg}: {pos}: {per_sec}")
            .unwrap()
            // For more spinners check out the cli-spinners project:
            // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
            .tick_strings(&[
                "▹▹▹▹▹",
                "▸▹▹▹▹",
                "▹▸▹▹▹",
                "▹▹▸▹▹",
                "▹▹▹▸▹",
                "▹▹▹▹▸",
                "▪▪▪▪▪",
            ]),
    );
    pb
}
