use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
};

use dozer_core::{
    epoch::Epoch,
    errors::ExecutionError,
    node::{PortHandle, Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::epoch::ExecutorOperation;
use dozer_types::indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use dozer_types::{
    bytes::{BufMut, BytesMut},
    types::{Operation, Schema},
};
use std::fs::OpenOptions;

#[derive(Debug, Clone)]
pub struct LogSinkSettings {
    pub file_buffer_capacity: u64,
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
    ) -> Result<(), ExecutionError> {
        debug_assert!(input_schemas.len() == 1);
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, ExecutionError> {
        Ok(Box::new(LogSink::new(
            Some(self.multi_pb.clone()),
            self.log_path.clone(),
            self.settings.file_buffer_capacity,
            self.endpoint_name.clone(),
        )?))
    }
}

#[derive(Debug)]
pub struct LogSink {
    pb: ProgressBar,
    buffered_file: BufWriter<File>,
    counter: usize,
}

impl LogSink {
    pub fn new(
        multi_pb: Option<MultiProgress>,
        log_path: PathBuf,
        file_buffer_capacity: u64,
        endpoint_name: String,
    ) -> Result<Self, ExecutionError> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(log_path)
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        let buffered_file = std::io::BufWriter::with_capacity(file_buffer_capacity as usize, file);

        let pb = attach_progress(multi_pb);
        pb.set_message(endpoint_name);

        Ok(Self {
            pb,
            buffered_file,
            counter: 0,
        })
    }
}

impl Sink for LogSink {
    fn process(&mut self, _from_port: PortHandle, op: Operation) -> Result<(), ExecutionError> {
        let msg = ExecutorOperation::Op { op };
        self.counter += 1;
        self.pb.set_position(self.counter as u64);
        write_msg_to_file(&mut self.buffered_file, &msg)
    }

    fn commit(&mut self) -> Result<(), ExecutionError> {
        let msg = ExecutorOperation::Commit {
            epoch: Epoch::new(0, Default::default()),
        };

        write_msg_to_file(&mut self.buffered_file, &msg)?;
        self.buffered_file
            .flush()
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
        Ok(())
    }

    fn on_source_snapshotting_done(
        &mut self,
        connection_name: String,
    ) -> Result<(), ExecutionError> {
        let msg = ExecutorOperation::SnapshottingDone { connection_name };
        write_msg_to_file(&mut self.buffered_file, &msg)
    }
}

fn write_msg_to_file(
    file: &mut BufWriter<File>,
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
