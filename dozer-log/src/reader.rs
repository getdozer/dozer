use crate::errors::ReaderBuilderError;
use crate::replication::LogOperation;
use crate::schemas::EndpointSchema;
use crate::storage::{LocalStorage, S3Storage, Storage};

use super::errors::ReaderError;
use dozer_types::grpc_types::internal::internal_pipeline_service_client::InternalPipelineServiceClient;
use dozer_types::grpc_types::internal::{
    storage_response, BuildRequest, LogRequest, LogResponse, StorageRequest,
};
use dozer_types::log::{debug, error};
use dozer_types::models::api_endpoint::{
    default_log_reader_batch_size, default_log_reader_buffer_size,
    default_log_reader_timeout_in_millis,
};
use dozer_types::tonic::transport::Channel;
use dozer_types::tonic::Streaming;
use dozer_types::{bincode, serde_json};
use tokio::select;
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
    pub schema: EndpointSchema,
    /// Protobuf descriptor of this endpoint's API.
    pub descriptor: Vec<u8>,
    pub options: LogReaderOptions,
    client: LogClient,
}

pub struct LogReader {
    /// Log server runs on a specific build of the endpoint. This is the name of the build.
    pub build_name: String,
    /// Schema of this endpoint.
    pub schema: EndpointSchema,
    /// Protobuf descriptor of this endpoint's API.
    pub descriptor: Vec<u8>,
    op_receiver: Receiver<OpAndPos>,
    worker: Option<JoinHandle<Result<(), ReaderError>>>,
}

impl LogReaderBuilder {
    pub async fn new(
        server_addr: String,
        options: LogReaderOptions,
    ) -> Result<Self, ReaderBuilderError> {
        let mut client = Self::get_client(server_addr).await?;
        let build = client
            .describe_build(BuildRequest {
                endpoint: options.endpoint.clone(),
            })
            .await?
            .into_inner();
        let build_name = build.name;
        let schema = serde_json::from_str(&build.schema_string)?;

        let client = LogClient::new(client, options.endpoint.clone()).await?;

        Ok(Self {
            build_name,
            schema,
            descriptor: build.descriptor_bytes,
            client,
            options,
        })
    }

    pub async fn get_client(
        server_addr: String,
    ) -> Result<InternalPipelineServiceClient<Channel>, ReaderBuilderError> {
        let client = InternalPipelineServiceClient::connect(server_addr).await?;
        Ok(client)
    }

    pub fn build(self, start: u64) -> LogReader {
        let LogReaderBuilder {
            build_name,
            schema,
            descriptor,
            client,
            options,
        } = self;

        let (op_sender, op_receiver) = tokio::sync::mpsc::channel(options.buffer_size as usize);
        let worker = tokio::spawn(log_reader_worker(client, start, options, op_sender));
        LogReader {
            build_name,
            schema,
            descriptor,
            op_receiver,
            worker: Some(worker),
        }
    }
}

/// An `LogOperation` and its position in the log.
pub struct OpAndPos {
    pub op: LogOperation,
    pub pos: u64,
}

impl LogReader {
    pub async fn read_one(&mut self) -> Result<OpAndPos, ReaderError> {
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
    client: InternalPipelineServiceClient<Channel>,
    request_sender: Sender<LogRequest>,
    response_stream: Streaming<LogResponse>,
    storage: Box<dyn Storage>,
}

impl LogClient {
    async fn new(
        mut client: InternalPipelineServiceClient<Channel>,
        endpoint: String,
    ) -> Result<Self, ReaderBuilderError> {
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

        let (request_sender, response_stream) = create_get_log_stream(&mut client).await?;

        Ok(Self {
            client,
            request_sender,
            response_stream,
            storage,
        })
    }

    async fn get_log(&mut self, request: LogRequest) -> Result<Vec<LogOperation>, ReaderError> {
        // Send the request.
        let response = loop {
            match call_get_log_once(
                &self.request_sender,
                request.clone(),
                &mut self.response_stream,
            )
            .await
            {
                Ok(response) => break response,
                Err(e) => {
                    error!("Error getting log: {:?}", e);
                    (self.request_sender, self.response_stream) = loop {
                        match create_get_log_stream(&mut self.client).await {
                            Ok((request_sender, response_stream)) => {
                                break (request_sender, response_stream)
                            }
                            Err(e) => {
                                const RETRY_INTERVAL: std::time::Duration =
                                    std::time::Duration::from_secs(5);
                                error!(
                                    "Error creating log stream: {e}, retrying after {RETRY_INTERVAL:?}..."
                                );
                                tokio::time::sleep(RETRY_INTERVAL).await;
                            }
                        }
                    }
                }
            }
        };
        use crate::replication::LogResponse;
        let response: LogResponse =
            bincode::deserialize(&response.data).map_err(ReaderError::DeserializeLogResponse)?;

        // Load response.
        let request_range = request.start..request.end;
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

async fn create_get_log_stream(
    client: &mut InternalPipelineServiceClient<Channel>,
) -> Result<(Sender<LogRequest>, Streaming<LogResponse>), dozer_types::tonic::Status> {
    let (request_sender, request_receiver) = tokio::sync::mpsc::channel::<LogRequest>(1);
    let request_stream = ReceiverStream::new(request_receiver);
    let response_stream = client.get_log(request_stream).await?.into_inner();
    Ok((request_sender, response_stream))
}

async fn call_get_log_once(
    request_sender: &Sender<LogRequest>,
    request: LogRequest,
    stream: &mut Streaming<LogResponse>,
) -> Result<LogResponse, Option<dozer_types::tonic::Status>> {
    if request_sender.send(request).await.is_err() {
        return Err(None);
    }
    match stream.next().await {
        Some(Ok(response)) => Ok(response),
        Some(Err(e)) => Err(Some(e)),
        None => Err(None),
    }
}

async fn log_reader_worker(
    log_client: LogClient,
    pos: u64,
    options: LogReaderOptions,
    op_sender: Sender<OpAndPos>,
) -> Result<(), ReaderError> {
    select! {
        _ = op_sender.closed() => {
            debug!("Log reader thread quit because LogReader was dropped");
            Ok(())
        }
        result = log_reader_worker_loop(log_client, pos, options, &op_sender) => {
            result
        }
    }
}

async fn log_reader_worker_loop(
    mut log_client: LogClient,
    mut pos: u64,
    options: LogReaderOptions,
    op_sender: &Sender<OpAndPos>,
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
            if op_sender.send(OpAndPos { op, pos }).await.is_err() {
                debug!("Log reader thread quit because LogReader was dropped");
                return Ok(());
            }
            pos += 1;
        }
    }
}
