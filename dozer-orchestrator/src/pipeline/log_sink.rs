use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
};

use dozer_api::grpc::internal::internal_pipeline_server::PipelineEventSenders;
use dozer_core::{
    epoch::Epoch,
    errors::ExecutionError,
    executor::ExecutorOperation,
    node::{PortHandle, Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::grpc_types::internal::StatusUpdate;
use dozer_types::indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use dozer_types::{
    bytes::{BufMut, BytesMut},
    models::api_endpoint::ApiEndpoint,
    types::{Operation, Schema},
};
use std::fs::OpenOptions;

use crate::utils::get_endpoint_log_path;

#[derive(Debug, Clone)]
pub struct LogSinkSettings {
    pub pipeline_dir: PathBuf,
    pub file_buffer_capacity: u64,
}

#[derive(Debug, Clone)]
pub struct LogSinkFactory {
    settings: LogSinkSettings,
    api_endpoint: ApiEndpoint,
    multi_pb: MultiProgress,
    notifier: Option<PipelineEventSenders>,
    notifier: Option<PipelineEventSenders>,
}

impl LogSinkFactory {
    pub fn new(
        settings: LogSinkSettings,
        api_endpoint: ApiEndpoint,
        multi_pb: MultiProgress,
        notifier: Option<PipelineEventSenders>,
        notifier: Option<PipelineEventSenders>,
    ) -> Self {
        Self {
            settings,
            api_endpoint,
            multi_pb,
            notifier,
            notifier,
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
            self.settings.file_buffer_capacity,
            self.api_endpoint.name.clone(),
            self.notifier.clone(),
        )?))
    }
}

#[derive(Debug)]
pub struct LogSink {
    pb: ProgressBar,
    buffered_file: BufWriter<File>,
    counter: usize,
    notifier: Option<PipelineEventSenders>,
    endpoint_name: String,
}

impl LogSink {
    pub fn new(
        multi_pb: Option<MultiProgress>,
        log_path: PathBuf,
        name: &str,
        file_buffer_capacity: u64,
        endpoint_name: String,
        notifier: Option<PipelineEventSenders>,
    ) -> Result<Self, ExecutionError> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(log_path)
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        let buffered_file = std::io::BufWriter::with_capacity(file_buffer_capacity as usize, file);

        let pb = attach_progress(multi_pb);
        pb.set_message(name.to_string());

        Ok(Self {
            pb,
            buffered_file,
            counter: 0,
            notifier,
            endpoint_name,
        })
    }
}

impl Sink for LogSink {
    fn process(&mut self, _from_port: PortHandle, op: Operation) -> Result<(), ExecutionError> {
        let msg = ExecutorOperation::Op { op };
        self.counter += 1;
        self.pb.set_position(self.counter as u64);
        if self.counter % 1000 == 0 {
            try_send(&self.notifier, self.counter, &self.endpoint_name);
        }
        write_msg_to_file(&mut self.buffered_file, &msg)
    }

    fn commit(&mut self) -> Result<(), ExecutionError> {
        let msg = ExecutorOperation::Commit {
            epoch: Epoch::new(0, Default::default()),
        };

        try_send(&self.notifier, self.counter, &self.endpoint_name);
        write_msg_to_file(&mut self.buffered_file, &msg)?;
        self.buffered_file.flush()?;
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self) -> Result<(), ExecutionError> {
        let msg = ExecutorOperation::SnapshottingDone {};
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

fn try_send(notifier: &Option<PipelineEventSenders>, progress: usize, endpoint_name: &str) {
    if let Some(n) = notifier {
        let status_update = StatusUpdate {
            source: endpoint_name.to_string(),
            r#type: "sink".to_string(),
            count: progress as i64,
        };

        let _ = n.2.try_send(status_update);
    }
}

fn try_send(notifier: &Option<PipelineEventSenders>, progress: usize, endpoint_name: &str) {
    if let Some(n) = notifier {
        let status_update = StatusUpdate {
            source: endpoint_name.to_string(),
            r#type: "sink".to_string(),
            count: progress as i64,
        };

        let _ = n.2.try_send(status_update);
    }
}
