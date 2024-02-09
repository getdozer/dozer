use std::sync::Arc;
use dozer_types::types::Field;
use dozer_log::{
    camino::Utf8Path,
    reader::{list_record_store_slices, processor_prefix},
    replication::create_data_storage,
    storage::{self, Object, Queue, Storage},
    tokio::task::JoinHandle,
};
use dozer_types::{
    bincode,
    log::info,
    models::app_config::DataStorage,
    node::{NodeHandle, OpIdentifier, SourceState, SourceStates},
    tonic::codegen::tokio_stream::StreamExt,
};
use tempdir::TempDir;

use crate::errors::ExecutionError;

#[derive(Debug)]
pub struct CheckpointFactory {
    queue: Queue,
    prefix: String,
}

#[derive(Debug, Clone)]
pub struct CheckpointFactoryOptions {
    pub persist_queue_capacity: usize,
}

impl Default for CheckpointFactoryOptions {
    fn default() -> Self {
        Self {
            persist_queue_capacity: 100,
        }
    }
}

#[derive(Debug, Clone)]
struct Checkpoint {
    processor_prefix: String,
    epoch_id: u64,
    source_states: SourceStates,
}

#[derive(Debug)]
pub struct OptionCheckpoint {
    storage: Box<dyn Storage>,
    prefix: String,
    checkpoint: Option<Checkpoint>,
}

#[derive(Debug, Clone, Default)]
pub struct CheckpointOptions {
    pub data_storage: DataStorage,
}

impl OptionCheckpoint {
    pub async fn new(
        checkpoint_dir: String,
        options: CheckpointOptions,
    ) -> Result<Self, ExecutionError> {
        let (storage, prefix) =
            create_data_storage(options.data_storage, checkpoint_dir.to_string()).await?;
        let checkpoint = read_record_store_slices(&*storage, &prefix).await?;
        if let Some(checkpoint) = &checkpoint {
            info!(
                "Restored record store from epoch id {}, processor states are stored in {}",
                checkpoint.epoch_id, checkpoint.processor_prefix
            );
        }

        Ok(Self {
            storage,
            prefix,
            checkpoint,
        })
    }

    pub fn storage(&self) -> &dyn Storage {
        &*self.storage
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn last_epoch_id(&self) -> Option<u64> {
        self.checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.epoch_id)
    }

    pub fn next_epoch_id(&self) -> u64 {
        self.last_epoch_id().map_or(0, |id| id + 1)
    }

    /// Returns the checkpointed source state, if any of the table is marked as non-restartable, returns `Err`.
    /// If there's no checkpoint or the node doesn't exist in the checkpoint's metadata, returns `Ok(None)`.
    pub fn get_source_state(
        &self,
        node_handle: &NodeHandle,
    ) -> Result<Option<OpIdentifier>, ExecutionError> {
        let Some(checkpoint) = self.checkpoint.as_ref() else {
            return Ok(None);
        };
        let Some(state) = checkpoint.source_states.get(node_handle) else {
            return Ok(None);
        };

        match state {
            SourceState::NotStarted => Ok(None),
            SourceState::NonRestartable => {
                Err(ExecutionError::SourceCannotRestart(node_handle.clone()))
            }
            SourceState::Restartable(id) => Ok(Some(*id)),
        }
    }

    pub async fn load_processor_data(
        &self,
        node_handle: &NodeHandle,
    ) -> Result<Option<Vec<u8>>, storage::Error> {
        if let Some(checkpoint) = &self.checkpoint {
            let key = processor_key(&checkpoint.processor_prefix, node_handle);
            info!("Loading processor {node_handle} checkpoint from {key}");
            self.storage.download_object(key).await.map(Some)
        } else {
            Ok(None)
        }
    }

    pub async fn load_record_writer_data(
        &self,
        node_handle: &NodeHandle,
        port_name: &str,
    ) -> Result<Option<Vec<u8>>, storage::Error> {
        if let Some(checkpoint) = &self.checkpoint {
            let key = record_writer_key(&checkpoint.processor_prefix, node_handle, port_name);
            info!("Loading record writer {node_handle}-{port_name} checkpoint from {key}");
            self.storage.download_object(key).await.map(Some)
        } else {
            Ok(None)
        }
    }
}

impl CheckpointFactory {
    // We need tokio runtime so mark the function as async.
    pub async fn new(
        checkpoint: OptionCheckpoint,
        options: CheckpointFactoryOptions,
    ) -> Result<(Self, JoinHandle<()>), ExecutionError> {
        let (queue, worker) = Queue::new(checkpoint.storage, options.persist_queue_capacity);

        Ok((
            Self {
                queue,
                prefix: checkpoint.prefix,
            },
            worker,
        ))
    }

    pub fn queue(&self) -> &Queue {
        &self.queue
    }
}

#[derive(Debug)]
struct CheckpointWriterFactoryState {}

#[derive(Debug, bincode::Encode, bincode::Decode)]
struct RecordStoreSlice {
    source_states: SourceStates,
    data: Vec<u8>,
}

#[derive(Debug)]
pub struct CheckpointWriter {
    factory: Arc<CheckpointFactory>,
    processor_prefix: String,
}

fn processor_key(processor_prefix: &str, node_handle: &NodeHandle) -> String {
    AsRef::<Utf8Path>::as_ref(processor_prefix)
        .join(node_handle.to_string())
        .into_string()
}

fn record_writer_key(processor_prefix: &str, node_handle: &NodeHandle, port_name: &str) -> String {
    AsRef::<Utf8Path>::as_ref(processor_prefix)
        .join(format!("{}-{}", node_handle, port_name))
        .into_string()
}

impl CheckpointWriter {
    pub fn new(factory: Arc<CheckpointFactory>, epoch_id: u64) -> Self {
        let processor_prefix = processor_prefix(&factory.prefix, epoch_id).into();
        Self {
            factory,
            processor_prefix,
        }
    }

    pub fn queue(&self) -> &Queue {
        &self.factory.queue
    }

    pub fn create_processor_object(
        &self,
        node_handle: &NodeHandle,
    ) -> Result<Object, ExecutionError> {
        let key = processor_key(&self.processor_prefix, node_handle);
        Object::new(self.factory.queue.clone(), key)
            .map_err(|_| ExecutionError::CheckpointWriterThreadPanicked)
    }

    pub fn create_record_writer_object(
        &self,
        node_handle: &NodeHandle,
        port_name: &str,
    ) -> Result<Object, ExecutionError> {
        let key = record_writer_key(&self.processor_prefix, node_handle, port_name);
        Object::new(self.factory.queue.clone(), key)
            .map_err(|_| ExecutionError::CheckpointWriterThreadPanicked)
    }
}

impl Drop for CheckpointWriter {
    fn drop(&mut self) {}
}

async fn read_record_store_slices(
    storage: &dyn Storage,
    factory_prefix: &str,
) -> Result<Option<Checkpoint>, ExecutionError> {
    let stream = list_record_store_slices(storage, factory_prefix);
    let mut stream = std::pin::pin!(stream);

    let mut last_checkpoint: Option<Checkpoint> = None;
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        info!("Loading {}", meta.key);
        let data = storage.download_object(meta.key).await?;
        let record_store_slice: RecordStoreSlice =
            bincode::decode_from_slice(&data, bincode::config::legacy())
                .map_err(ExecutionError::CorruptedCheckpoint)?
                .0;
        last_checkpoint = Some(Checkpoint {
            epoch_id: meta.epoch_id,
            source_states: record_store_slice.source_states,
            processor_prefix: meta.processor_prefix.into(),
        });
    }

    Ok(last_checkpoint)
}

/// This is only meant to be used in tests.
pub async fn create_checkpoint_for_test() -> (TempDir, OptionCheckpoint) {
    let temp_dir = TempDir::new("create_checkpoint_for_test").unwrap();
    let checkpoint_dir = temp_dir.path().to_str().unwrap().to_string();
    let checkpoint = OptionCheckpoint::new(checkpoint_dir.clone(), Default::default())
        .await
        .unwrap();
    (temp_dir, checkpoint)
}

/// This is only meant to be used in tests.
pub async fn create_checkpoint_factory_for_test(
    _records: &[Vec<Field>],
) -> (TempDir, Arc<CheckpointFactory>, JoinHandle<()>) {
    // Create empty checkpoint storage.
    let temp_dir = TempDir::new("create_checkpoint_factory_for_test").unwrap();
    let checkpoint_dir = temp_dir.path().to_str().unwrap().to_string();
    let checkpoint = OptionCheckpoint::new(checkpoint_dir.clone(), Default::default())
        .await
        .unwrap();
    let (checkpoint_factory, handle) = CheckpointFactory::new(checkpoint, Default::default())
        .await
        .unwrap();
    let factory = Arc::new(checkpoint_factory);

    let epoch_id = 42;
    let source_states: SourceStates = [(
        NodeHandle::new(Some(1), "id".to_string()),
        SourceState::NotStarted,
    )]
    .into_iter()
    .collect();
    std::thread::spawn(move || drop(CheckpointWriter::new(factory, epoch_id)))
        .join()
        .unwrap();
    handle.await.unwrap();

    // Create a new factory that loads from the checkpoint.
    let checkpoint = OptionCheckpoint::new(checkpoint_dir, Default::default())
        .await
        .unwrap();
    let last_checkpoint = checkpoint.checkpoint.as_ref().unwrap();
    assert_eq!(last_checkpoint.epoch_id, epoch_id);
    assert_eq!(last_checkpoint.source_states, source_states);
    let (checkpoint_factory, handle) = CheckpointFactory::new(checkpoint, Default::default())
        .await
        .unwrap();
    (temp_dir, Arc::new(checkpoint_factory), handle)
}

pub mod serialize;

#[cfg(test)]
mod tests {
    use super::*;

    use dozer_log::tokio;

    #[tokio::test]
    async fn checkpoint_writer_should_write_records() {
        create_checkpoint_factory_for_test(&[vec![Field::Int(0)]]).await;
    }
}
