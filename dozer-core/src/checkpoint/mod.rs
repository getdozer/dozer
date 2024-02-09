use std::{ops::Deref, sync::Arc};

use dozer_log::{
    camino::Utf8Path,
    reader::{list_record_store_slices, processor_prefix, record_store_key},
    replication::create_data_storage,
    storage::{self, Object, Queue, Storage},
    tokio::task::JoinHandle,
};
use dozer_recordstore::{ProcessorRecordStore, ProcessorRecordStoreDeserializer, StoreRecord};
use dozer_types::{
    bincode,
    log::{error, info},
    models::app_config::{DataStorage, RecordStore},
    node::{NodeHandle, OpIdentifier, SourceState, SourceStates},
    parking_lot::Mutex,
    tonic::codegen::tokio_stream::StreamExt,
    types::Field,
};
use tempdir::TempDir;

use crate::errors::ExecutionError;

#[derive(Debug)]
pub struct CheckpointFactory {
    queue: Queue,
    prefix: String,
    record_store: Arc<ProcessorRecordStore>,
    state: Mutex<CheckpointWriterFactoryState>,
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
    record_store: ProcessorRecordStoreDeserializer,
    checkpoint: Option<Checkpoint>,
}

#[derive(Debug, Clone, Default)]
pub struct CheckpointOptions {
    pub data_storage: DataStorage,
    pub record_store: RecordStore,
}

impl OptionCheckpoint {
    pub async fn new(
        checkpoint_dir: String,
        options: CheckpointOptions,
    ) -> Result<Self, ExecutionError> {
        let (storage, prefix) =
            create_data_storage(options.data_storage, checkpoint_dir.to_string()).await?;
        let (record_store, checkpoint) =
            read_record_store_slices(&*storage, &prefix, options.record_store).await?;
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
            record_store,
        })
    }

    pub fn storage(&self) -> &dyn Storage {
        &*self.storage
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn record_store(&self) -> &ProcessorRecordStoreDeserializer {
        &self.record_store
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

        let record_store = checkpoint.record_store.into_record_store();
        let state = Mutex::new(CheckpointWriterFactoryState {
            next_record_index: record_store.num_records(),
        });

        Ok((
            Self {
                queue,
                prefix: checkpoint.prefix,
                record_store: Arc::new(record_store),
                state,
            },
            worker,
        ))
    }

    pub fn queue(&self) -> &Queue {
        &self.queue
    }

    pub fn record_store(&self) -> &Arc<ProcessorRecordStore> {
        &self.record_store
    }

    fn write_record_store_slice(
        &self,
        key: String,
        source_states: SourceStates,
    ) -> Result<(), ExecutionError> {
        let mut state = self.state.lock();
        let (data, next_record_index) =
            self.record_store.serialize_slice(state.next_record_index)?;
        state.next_record_index = next_record_index;
        drop(state);

        let data = bincode::encode_to_vec(
            RecordStoreSlice {
                source_states,
                data,
            },
            bincode::config::legacy(),
        )
        .expect("Record store slice should be serializable");
        self.queue
            .upload_object(key, data)
            .map_err(|_| ExecutionError::CheckpointWriterThreadPanicked)?;
        Ok(())
    }
}

#[derive(Debug)]
struct CheckpointWriterFactoryState {
    next_record_index: usize,
}

#[derive(Debug, bincode::Encode, bincode::Decode)]
struct RecordStoreSlice {
    source_states: SourceStates,
    data: Vec<u8>,
}

#[derive(Debug)]
pub struct CheckpointWriter {
    factory: Arc<CheckpointFactory>,
    record_store_key: String,
    source_states: Arc<SourceStates>,
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
    pub fn new(
        factory: Arc<CheckpointFactory>,
        epoch_id: u64,
        source_states: Arc<SourceStates>,
    ) -> Self {
        let record_store_key = record_store_key(&factory.prefix, epoch_id).into();
        let processor_prefix = processor_prefix(&factory.prefix, epoch_id).into();
        Self {
            factory,
            record_store_key,
            source_states,
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

    fn drop(&mut self) -> Result<(), ExecutionError> {
        self.factory.write_record_store_slice(
            std::mem::take(&mut self.record_store_key),
            self.source_states.deref().clone(),
        )
    }
}

impl Drop for CheckpointWriter {
    fn drop(&mut self) {
        if let Err(e) = self.drop() {
            error!("Failed to write record store slice: {:?}", e);
        }
    }
}

async fn read_record_store_slices(
    storage: &dyn Storage,
    factory_prefix: &str,
    record_store: RecordStore,
) -> Result<(ProcessorRecordStoreDeserializer, Option<Checkpoint>), ExecutionError> {
    let record_store = ProcessorRecordStoreDeserializer::new(record_store)?;

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
        record_store.deserialize_and_extend(&record_store_slice.data)?;
        last_checkpoint = Some(Checkpoint {
            epoch_id: meta.epoch_id,
            source_states: record_store_slice.source_states,
            processor_prefix: meta.processor_prefix.into(),
        });
    }

    Ok((record_store, last_checkpoint))
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
    records: &[Vec<Field>],
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

    // Write data to checkpoint.
    for record in records {
        factory.record_store().create_ref(record).unwrap();
    }
    // Writer must be dropped outside tokio context.
    let epoch_id = 42;
    let source_states: SourceStates = [(
        NodeHandle::new(Some(1), "id".to_string()),
        SourceState::NotStarted,
    )]
    .into_iter()
    .collect();
    let source_states_clone = Arc::new(source_states.clone());
    std::thread::spawn(move || {
        drop(CheckpointWriter::new(
            factory,
            epoch_id,
            source_states_clone,
        ))
    })
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
