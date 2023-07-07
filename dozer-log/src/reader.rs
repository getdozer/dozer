use crate::attach_progress;
use crate::storage::{LocalStorage, S3Storage, Storage};

use super::errors::ReaderError;
use dozer_types::bincode;
use dozer_types::epoch::ExecutorOperation;
use dozer_types::grpc_types::internal::internal_pipeline_service_client::InternalPipelineServiceClient;
use dozer_types::grpc_types::internal::{
    storage_response, LogRequest, LogResponse, StorageRequest,
};
use dozer_types::indicatif::{MultiProgress, ProgressBar};
use dozer_types::log::debug;
use dozer_types::tonic::Streaming;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

pub struct LogReaderOptions {
    pub endpoint: String,
    pub batch_size: u32,
    pub timeout_in_millis: u32,
    pub buffer_size: u32,
}

#[derive(Debug)]
pub struct LogReader {
    op_receiver: Receiver<(ExecutorOperation, u64)>,
    worker: Option<JoinHandle<Result<(), ReaderError>>>,
}

impl LogReader {
    pub async fn new(
        server_addr: String,
        options: LogReaderOptions,
        pos: u64,
        multi_pb: Option<MultiProgress>,
    ) -> Result<Self, ReaderError> {
        let pb = attach_progress(multi_pb);
        pb.set_message(format!("reader: {}", options.endpoint));
        pb.set_position(pos);

        let client = LogClient::new(server_addr, options.endpoint.clone()).await?;

        let (op_sender, op_receiver) =
            tokio::sync::mpsc::channel::<(ExecutorOperation, u64)>(options.buffer_size as usize);
        let worker = tokio::spawn(log_reader_worker(client, pos, pb, options, op_sender));

        Ok(Self {
            op_receiver,
            worker: Some(worker),
        })
    }

    /// Returns an op and the position of next op.
    pub async fn next_op(&mut self) -> Result<(ExecutorOperation, u64), ReaderError> {
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

struct LogClient {
    request_sender: Sender<LogRequest>,
    response_stream: Streaming<LogResponse>,
    storage: Box<dyn Storage>,
}

impl LogClient {
    async fn new(server_addr: String, endpoint: String) -> Result<Self, ReaderError> {
        let mut client = InternalPipelineServiceClient::connect(server_addr).await?;
        let storage = client
            .describe_storage(StorageRequest { endpoint })
            .await?
            .into_inner();
        let storage: Box<dyn Storage> = match storage.storage.expect("Must not be None") {
            storage_response::Storage::S3(s3) => Box::new(S3Storage::new(s3.bucket_name).await?),
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

    async fn get_log(
        &mut self,
        request: LogRequest,
    ) -> Result<Vec<ExecutorOperation>, ReaderError> {
        // Send the request.
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
                let data = self.storage.download_object(persisted.key).await?;
                bincode::deserialize(&data).map_err(ReaderError::DeserializeLogEntry)
            }
            LogResponse::Operations(ops) => Ok(ops),
        }
    }
}

async fn log_reader_worker(
    mut log_client: LogClient,
    mut pos: u64,
    pb: ProgressBar,
    options: LogReaderOptions,
    op_sender: Sender<(ExecutorOperation, u64)>,
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
