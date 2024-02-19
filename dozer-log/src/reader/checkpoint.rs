use async_stream::try_stream;
use camino::{Utf8Path, Utf8PathBuf};
use dozer_types::{
    grpc_types::internal::{
        internal_pipeline_service_client::InternalPipelineServiceClient, BuildRequest,
        StorageRequest,
    },
    thiserror,
    tonic::{
        self,
        transport::{self, Channel},
    },
};
use futures_util::{Stream, StreamExt};

use crate::{
    reader::create_storage,
    replication::{self, load_persisted_log_entries, PersistedLogEntry},
    schemas::SinkSchema,
    storage::{self, Storage},
};

#[derive(Debug)]
pub struct CheckpointedLogReader {
    client: InternalPipelineServiceClient<Channel>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("connect to internal service: {0}")]
    Connect(#[from] transport::Error),
    #[error("describe storage of endpoint {0}: {1}")]
    DescribeStorage(String, #[source] tonic::Status),
    #[error("describe schema of endpoint {0}: {1}")]
    DescribeSchema(String, #[source] tonic::Status),
    #[error("invalid schema of endpoint {0}: {1}")]
    InvalidSchema(String, #[source] serde_json::Error),
    #[error("storage: {0}")]
    Storage(#[from] storage::Error),
    #[error("unrecognized checkpoint: {0}")]
    UnrecognizedCheckpoint(String),
    #[error("replication error: {0}")]
    Replication(#[from] replication::Error),
}

impl CheckpointedLogReader {
    pub async fn new(app_server_url: String) -> Result<Self, Error> {
        let client = InternalPipelineServiceClient::connect(app_server_url).await?;
        Ok(Self { client })
    }

    pub async fn list_entries(
        &mut self,
        endpoint: String,
    ) -> Result<(Box<dyn Storage>, Vec<PersistedLogEntry>), Error> {
        // Request storage information.
        let storage_response = self
            .client
            .describe_storage(StorageRequest {
                endpoint: endpoint.clone(),
            })
            .await
            .map_err(|e| Error::DescribeStorage(endpoint.clone(), e))?
            .into_inner();
        let storage = create_storage(storage_response.storage.expect("Must not be None")).await?;

        // Find last epoch id.
        let mut last_epoch_id = None;
        {
            let stream = list_record_store_slices(&*storage, &storage_response.checkpoint_prefix);
            let mut stream = std::pin::pin!(stream);
            while let Some(meta) = stream.next().await {
                let meta = meta?;
                last_epoch_id = Some(meta.epoch_id);
            }
        }

        // Load persisted log entries.
        let entries =
            load_persisted_log_entries(&*storage, storage_response.log_prefix, last_epoch_id)
                .await?;

        Ok((storage, entries))
    }

    pub async fn get_schema(&mut self, endpoint: String) -> Result<SinkSchema, Error> {
        let build = self
            .client
            .describe_build(BuildRequest {
                endpoint: endpoint.clone(),
            })
            .await
            .map_err(|e| Error::DescribeSchema(endpoint.clone(), e))?
            .into_inner();
        let schema = serde_json::from_str(&build.schema_string)
            .map_err(|e| Error::InvalidSchema(endpoint.clone(), e))?;
        Ok(schema)
    }
}

pub struct RecordStoreSliceMeta {
    pub key: String,
    pub epoch_id: u64,
    pub processor_prefix: Utf8PathBuf,
}

pub fn list_record_store_slices<'a>(
    storage: &'a dyn Storage,
    checkpoint_prefix: &'a str,
) -> impl Stream<Item = Result<RecordStoreSliceMeta, Error>> + 'a {
    let record_store_prefix = record_store_prefix(checkpoint_prefix);
    let mut continuation_token = None;
    try_stream! {
        loop {
            let objects = storage
                .list_objects(record_store_prefix.to_string(), continuation_token)
                .await?;

            for object in objects.objects {
                let object_name = AsRef::<Utf8Path>::as_ref(&object.key)
                    .strip_prefix(&record_store_prefix)
                    .map_err(|_| Error::UnrecognizedCheckpoint(object.key.clone()))?;
                let epoch_id = object_name
                    .as_str()
                    .parse()
                    .map_err(|_| Error::UnrecognizedCheckpoint(object.key.clone()))?;
                let processor_prefix = processor_prefix(checkpoint_prefix, epoch_id);
                yield RecordStoreSliceMeta {
                    key: object.key,
                    epoch_id,
                    processor_prefix,
                };
            }

            continuation_token = objects.continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }
    }
}

pub fn processor_prefix(checkpoint_prefix: &str, epoch_id: u64) -> Utf8PathBuf {
    AsRef::<Utf8Path>::as_ref(checkpoint_prefix).join(format!("{:020}", epoch_id))
}

fn record_store_prefix(checkpoint_prefix: &str) -> Utf8PathBuf {
    AsRef::<Utf8Path>::as_ref(checkpoint_prefix).join("record_store")
}

pub fn record_store_key(checkpoint_prefix: &str, epoch_id: u64) -> Utf8PathBuf {
    // Format with `u64` max number of digits.
    record_store_prefix(checkpoint_prefix).join(format!("{:020}", epoch_id))
}

#[cfg(test)]
mod tests {
    use crate::replication::load_persisted_log_entry;

    use super::*;

    #[tokio::test]
    #[ignore = "this is an example of using `CheckpointedLogReader`"]
    async fn test_checkpointed_log_reader() {
        let mut reader = CheckpointedLogReader::new("http://localhost:50051".into())
            .await
            .unwrap();
        let endpoint = "test_endpoint";

        let mut current_end = 0;
        loop {
            let (storage, entries) = reader.list_entries(endpoint.to_string()).await.unwrap();
            let new_entries = entries
                .into_iter()
                .filter(|entry| entry.range.start > current_end)
                .collect::<Vec<_>>();
            for new_entry in &new_entries {
                let _operations = load_persisted_log_entry(&*storage, new_entry)
                    .await
                    .unwrap();
            }

            if let Some(entry) = new_entries.last() {
                current_end = entry.range.end;
            }

            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    }
}
