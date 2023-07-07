use std::ops::Range;

use crate::attach_progress;
use crate::replication::LogResponse;
use crate::storage::{LocalStorage, S3Storage, Storage};

use super::errors::ReaderError;
use dozer_types::bincode;
use dozer_types::epoch::ExecutorOperation;
use dozer_types::grpc_types::internal::internal_pipeline_service_client::InternalPipelineServiceClient;
use dozer_types::grpc_types::internal::{storage_response, LogRequest, StorageRequest};
use dozer_types::indicatif::{MultiProgress, ProgressBar};
use dozer_types::tonic::Streaming;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

#[derive(Debug)]
struct PersistedOps {
    range: Range<usize>,
    ops: Vec<ExecutorOperation>,
}

pub struct LogReaderOptions {
    pub endpoint: String,
    pub timeout_in_millis: u32,
}

#[derive(Debug)]
pub struct LogReader {
    pos: u64,
    request_sender: tokio::sync::mpsc::Sender<Range<usize>>,
    response_stream: Streaming<dozer_types::grpc_types::internal::LogResponse>,
    storage: Box<dyn Storage>,
    persisted: Option<PersistedOps>,
    pb: ProgressBar,
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

        let mut client = InternalPipelineServiceClient::connect(server_addr).await?;
        let storage = client
            .describe_storage(StorageRequest {
                endpoint: options.endpoint.clone(),
            })
            .await?
            .into_inner();
        let storage: Box<dyn Storage> = match storage.storage.expect("Must not be None") {
            storage_response::Storage::S3(s3) => Box::new(S3Storage::new(s3.bucket_name).await?),
            storage_response::Storage::Local(local) => {
                Box::new(LocalStorage::new(local.root).await?)
            }
        };

        let (request_sender, request_receiver) = tokio::sync::mpsc::channel::<Range<usize>>(1);
        let request_stream = ReceiverStream::new(request_receiver).map(move |request| LogRequest {
            endpoint: options.endpoint.clone(),
            start: request.start as u64,
            end: request.end as u64,
            timeout_in_millis: options.timeout_in_millis,
        });
        let response_stream = client.get_log(request_stream).await?.into_inner();

        Ok(Self {
            request_sender,
            response_stream,
            pos,
            storage,
            persisted: None,
            pb,
        })
    }

    pub async fn next_op(&mut self) -> Result<(ExecutorOperation, u64), ReaderError> {
        let pos = self.pos as usize;

        // First check if we have any persisted ops.
        if let Some(persisted) = self.persisted.as_ref() {
            if persisted.range.contains(&pos) {
                return Ok((
                    op_from_persisted(persisted, &mut self.pos, &self.pb),
                    self.pos,
                ));
            }
        }

        // Send the request.
        let request = pos..pos + 1;
        self.request_sender
            .send(request)
            .await
            .expect("We're holding the receiver");
        let response = self
            .response_stream
            .next()
            .await
            .ok_or(ReaderError::UnexpectedEndOfStream)??;
        let response: LogResponse =
            bincode::deserialize(&response.data).map_err(ReaderError::DeserializeLogResponse)?;

        // Load response.
        match response {
            LogResponse::Persisted(persisted) => {
                let data = self.storage.download_object(persisted.key).await?;
                let ops: Vec<ExecutorOperation> =
                    bincode::deserialize(&data).map_err(ReaderError::DeserializeLogEntry)?;
                let persisted = PersistedOps {
                    range: persisted.range,
                    ops,
                };
                let op = op_from_persisted(&persisted, &mut self.pos, &self.pb);
                self.persisted = Some(persisted);
                Ok((op, self.pos))
            }
            LogResponse::Operations(mut ops) => {
                debug_assert!(ops.len() == 1);
                self.pb.set_position(self.pos);
                self.pos += 1;
                Ok((ops.remove(0), self.pos))
            }
        }
    }
}

fn op_from_persisted(
    persisted: &PersistedOps,
    pos: &mut u64,
    pb: &ProgressBar,
) -> ExecutorOperation {
    let pos_usize = *pos as usize;
    debug_assert!(persisted.range.contains(&pos_usize));
    let op = persisted.ops[pos_usize - persisted.range.start].clone();
    pb.set_position(*pos);
    *pos += 1;
    op
}
