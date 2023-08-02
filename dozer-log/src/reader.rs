use crate::attach_progress;
use crate::replication::LogOperation;
use crate::schemas::BuildSchema;
use crate::storage::{LocalStorage, S3Storage, Storage};

use super::errors::ReaderError;
use dozer_types::grpc_types::internal::internal_pipeline_service_client::InternalPipelineServiceClient;
use dozer_types::grpc_types::internal::{
    storage_response, BuildRequest, LogRequest, LogResponse, StorageRequest,
};
use dozer_types::indicatif::{MultiProgress, ProgressBar};
use dozer_types::log::debug;
use dozer_types::models::api_endpoint::{
    default_log_reader_batch_size, default_log_reader_buffer_size,
    default_log_reader_timeout_in_millis,
};
use dozer_types::tonic::transport::Channel;
use dozer_types::tonic::Streaming;
use dozer_types::{bincode, serde_json};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

#[derive(Debug)]
pub struct LogReaderOptions {
    pub endpoint: String,
    pub batch_size: u32,
    pub timeout_in_millis: u32,
    pub buffer_size: u32,
}

impl LogReaderOptions {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            batch_size: default_log_reader_batch_size(),
            timeout_in_millis: default_log_reader_timeout_in_millis(),
            buffer_size: default_log_reader_buffer_size(),
        }
    }
}

#[derive(Debug)]
pub struct LogReaderBuilder {
    /// Log server runs on a specific build of the endpoint. This is the name of the build.
    pub build_name: String,
    /// Schema of this endpoint.
    pub schema: BuildSchema,
    /// Protobuf descriptor of this endpoint's API.
    pub descriptor: Vec<u8>,
    pub options: LogReaderOptions,
    client: LogClient,
}

pub struct LogReader {
    /// Log server runs on a specific build of the endpoint. This is the name of the build.
    pub build_name: String,
    /// Schema of this endpoint.
    pub schema: BuildSchema,
    /// Protobuf descriptor of this endpoint's API.
    pub descriptor: Vec<u8>,
    op_receiver: Receiver<(LogOperation, u64)>,
    worker: Option<JoinHandle<Result<(), ReaderError>>>,
}

impl LogReaderBuilder {
    pub async fn new(server_addr: String, options: LogReaderOptions) -> Result<Self, ReaderError> {
        let mut client = InternalPipelineServiceClient::connect(server_addr).await?;
        let build = client
            .describe_build(BuildRequest {
                endpoint: options.endpoint.clone(),
            })
            .await?
            .into_inner();
        let build_name = build.name;
        let schema = serde_json::from_str(&build.schema_string)?;

        let client = LogClient::new(&mut client, options.endpoint.clone()).await?;

        Ok(Self {
            build_name,
            schema,
            descriptor: build.descriptor_bytes,
            client,
            options,
        })
    }

    pub fn build(self, pos: u64, multi_pb: Option<MultiProgress>) -> LogReader {
        let LogReaderBuilder {
            build_name,
            schema,
            descriptor,
            client,
            options,
        } = self;
        let pb = attach_progress(multi_pb);
        pb.set_message(format!("reader: {}", options.endpoint));
        pb.set_position(pos);

        let (op_sender, op_receiver) =
            tokio::sync::mpsc::channel::<(LogOperation, u64)>(options.buffer_size as usize);
        let worker = tokio::spawn(log_reader_worker(client, pos, pb, options, op_sender));
        LogReader {
            build_name,
            schema,
            descriptor,
            op_receiver,
            worker: Some(worker),
        }
    }
}

impl LogReader {
    /// Returns an op and the position of next op.
    pub async fn next_op(&mut self) -> Result<(LogOperation, u64), ReaderError> {
        if let Some(result) = self.op_receiver.recv().await {
            Ok(result)
        } else if let Some(worker) = self.worker.take() {
            match worker.await {
                Ok(Ok(())) => {
                    panic!("Worker never quit without an error because we're holding the receiver")
                }
                Ok(Err(e)) => Err(e),
                Err(e) => Err(ReaderError::ReaderThreadQuit(Some(e))),
            }
        } else {
            Err(ReaderError::ReaderThreadQuit(None))
        }
    }
}

#[derive(Debug)]
struct LogClient {
    request_sender: Sender<LogRequest>,
    response_stream: Streaming<LogResponse>,
    storage: Box<dyn Storage>,
}

impl LogClient {
    async fn new(
        client: &mut InternalPipelineServiceClient<Channel>,
        endpoint: String,
    ) -> Result<Self, ReaderError> {
        let storage = client
            .describe_storage(StorageRequest { endpoint })
            .await?
            .into_inner();
        let storage: Box<dyn Storage> = match storage.storage.expect("Must not be None") {
            storage_response::Storage::S3(s3) => {
                Box::new(S3Storage::new(s3.region.as_str().into(), s3.bucket_name).await?)
            }
            storage_response::Storage::Local(local) => {
                Box::new(LocalStorage::new(local.root).await?)
            }
        };

        let (request_sender, request_receiver) = tokio::sync::mpsc::channel::<LogRequest>(1);
        let request_stream = ReceiverStream::new(request_receiver);
        let response_stream = client.get_log(request_stream).await?.into_inner();

        Ok(Self {
            request_sender,
            response_stream,
            storage,
        })
    }

    async fn get_log(&mut self, request: LogRequest) -> Result<Vec<LogOperation>, ReaderError> {
        // Send the request.
        let request_range = request.start..request.end;
        self.request_sender
            .send(request)
            .await
            .expect("We're holding the receiver");
        let response = self
            .response_stream
            .next()
            .await
            .ok_or(ReaderError::UnexpectedEndOfStream)??;
        use crate::replication::LogResponse;
        let response: LogResponse =
            bincode::deserialize(&response.data).map_err(ReaderError::DeserializeLogResponse)?;

        // Load response.
        match response {
            LogResponse::Persisted(persisted) => {
                debug!(
                    "Loading persisted log entry {}, entry range {:?}, requested range {:?}",
                    persisted.key, persisted.range, request_range
                );
                // Load the persisted log entry.
                let data = self.storage.download_object(persisted.key).await?;
                let mut ops: Vec<LogOperation> =
                    bincode::deserialize(&data).map_err(ReaderError::DeserializeLogEntry)?;
                // Discard the ops that are before the requested range.
                ops.drain(..request_range.start as usize - persisted.range.start);
                Ok(ops)
            }
            LogResponse::Operations(ops) => {
                debug!(
                    "Got {} ops for request range {:?}",
                    ops.len(),
                    request_range
                );
                Ok(ops)
            }
        }
    }
}

async fn log_reader_worker(
    mut log_client: LogClient,
    mut pos: u64,
    pb: ProgressBar,
    options: LogReaderOptions,
    op_sender: Sender<(LogOperation, u64)>,
) -> Result<(), ReaderError> {
    loop {
        // Request ops.
        let request = LogRequest {
            endpoint: options.endpoint.clone(),
            start: pos,
            end: pos + options.batch_size as u64,
            timeout_in_millis: options.timeout_in_millis,
        };
        let ops = log_client.get_log(request).await?;

        for op in ops {
            pos += 1;
            pb.set_position(pos);
            if op_sender.send((op, pos)).await.is_err() {
                debug!("Log reader thread quit because LogReader was dropped");
                return Ok(());
            }
        }
    }
}
