use std::{collections::HashMap, num::NonZeroUsize, ops::Deref, sync::Arc};

use dozer_log::{
    camino::{Utf8Path, Utf8PathBuf},
    replication::create_data_storage,
    storage::{self, Object, Queue, Storage},
    tokio::task::JoinHandle,
};
use dozer_recordstore::{ProcessorRecordStore, ProcessorRecordStoreDeserializer, StoreRecord};
use dozer_types::{
    bincode,
    log::{error, info},
    models::app_config::{DataStorage, RecordStore},
    node::{NodeHandle, OpIdentifier, SourceStates, TableState},
    parking_lot::Mutex,
    serde::{Deserialize, Serialize},
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
    /// The number of slices that the record store is split into.
    num_slices: NonZeroUsize,
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
                "Restored record store from {}th checkpoint, last epoch id is {}, processor states are stored in {}",
                checkpoint.num_slices, checkpoint.epoch_id, checkpoint.processor_prefix
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

    pub fn num_slices(&self) -> usize {
        self.checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.num_slices.get())
    }

    pub fn next_epoch_id(&self) -> u64 {
        self.checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.epoch_id + 1)
    }

    /// Returns the checkpointed source state, if any of the table is marked as non-restartable, returns `Err`.
    /// If there's no checkpoint or the node doesn't exist in the checkpoint's metadata, returns `Ok(None)`.
    pub fn get_source_state(
        &self,
        node_handle: &NodeHandle,
    ) -> Result<Option<HashMap<String, Option<OpIdentifier>>>, ExecutionError> {
        let Some(checkpoint) = self.checkpoint.as_ref() else {
            return Ok(None);
        };
        let Some(source_state) = checkpoint.source_states.get(node_handle) else {
            return Ok(None);
        };

        let mut result = HashMap::new();
        for (table_name, state) in source_state {
            let id = match state {
                TableState::NotStarted => None,
                TableState::NonRestartable => {
                    return Err(ExecutionError::SourceCannotRestart {
                        source_name: node_handle.clone(),
                        table_name: table_name.clone(),
                    });
                }
                TableState::Restartable(id) => Some(*id),
            };
            result.insert(table_name.clone(), id);
        }
        Ok(Some(result))
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

    pub fn record_store(&self) -> &Arc<ProcessorRecordStore> {
        &self.record_store
    }

    fn write_record_store_slice(
        &self,
        key: String,
        source_states: SourceStates,
    ) -> Result<(), ExecutionError> {
        let mut state = self.state.lock();
        let (data, num_records_serialized) =
            self.record_store.serialize_slice(state.next_record_index)?;
        state.next_record_index += num_records_serialized;
        drop(state);

        let data = bincode::serialize(&RecordStoreSlice {
            source_states,
            data,
        })
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
struct RecordStoreSlice {
    source_states: SourceStates,
    #[serde(with = "dozer_types::serde_bytes")]
    data: Vec<u8>,
}

#[derive(Debug)]
pub struct CheckpointWriter {
    factory: Arc<CheckpointFactory>,
    record_store_key: String,
    source_states: Arc<SourceStates>,
    processor_prefix: String,
}

fn record_store_prefix(factory_prefix: &str) -> Utf8PathBuf {
    AsRef::<Utf8Path>::as_ref(factory_prefix).join("record_store")
}

fn processor_prefix(factory_prefix: &str, epoch_id: &str) -> String {
    AsRef::<Utf8Path>::as_ref(factory_prefix)
        .join(epoch_id)
        .into_string()
}

fn processor_key(processor_prefix: &str, node_handle: &NodeHandle) -> String {
    AsRef::<Utf8Path>::as_ref(processor_prefix)
        .join(node_handle.to_string())
        .into_string()
}

impl CheckpointWriter {
    pub fn new(
        factory: Arc<CheckpointFactory>,
        epoch_id: u64,
        source_states: Arc<SourceStates>,
    ) -> Self {
        // Format with `u64` max number of digits.
        let epoch_id = format!("{:020}", epoch_id);
        let record_store_key = record_store_prefix(&factory.prefix)
            .join(&epoch_id)
            .into_string();
        let processor_prefix = processor_prefix(&factory.prefix, &epoch_id);
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
    let record_store_prefix = record_store_prefix(factory_prefix);

    let mut last_checkpoint: Option<Checkpoint> = None;
    let mut continuation_token = None;
    loop {
        let objects = storage
            .list_objects(record_store_prefix.to_string(), continuation_token)
            .await?;

        if let Some(object) = objects.objects.last() {
            let object_name = AsRef::<Utf8Path>::as_ref(&object.key)
                .strip_prefix(&record_store_prefix)
                .map_err(|_| ExecutionError::UnrecognizedCheckpoint(object.key.clone()))?;
            let epoch_id = object_name
                .as_str()
                .parse()
                .map_err(|_| ExecutionError::UnrecognizedCheckpoint(object.key.clone()))?;
            info!("Loading {}", object.key);
            let data = storage.download_object(object.key.clone()).await?;
            let record_store_slice = bincode::deserialize::<RecordStoreSlice>(&data)
                .map_err(ExecutionError::CorruptedCheckpoint)?;
            let processor_prefix = processor_prefix(factory_prefix, object_name.as_str());

            if let Some(last_checkpoint) = last_checkpoint.as_mut() {
                last_checkpoint.num_slices = last_checkpoint
                    .num_slices
                    .checked_add(objects.objects.len())
                    .expect("shouldn't overflow");
                last_checkpoint.epoch_id = epoch_id;
                last_checkpoint.source_states = record_store_slice.source_states;
                last_checkpoint.processor_prefix = processor_prefix;
            } else {
                info!(
                    "Current source states are {:?}",
                    record_store_slice.source_states
                );
                last_checkpoint = Some(Checkpoint {
                    num_slices: NonZeroUsize::new(objects.objects.len())
                        .expect("have at least one element"),
                    epoch_id,
                    source_states: record_store_slice.source_states,
                    processor_prefix,
                });
            }
        }

        for object in objects.objects {
            info!("Loading {}", object.key);
            let data = storage.download_object(object.key).await?;
            let record_store_slice = bincode::deserialize::<RecordStoreSlice>(&data)
                .map_err(ExecutionError::CorruptedCheckpoint)?;
            record_store.deserialize_and_extend(&record_store_slice.data)?;
        }

        continuation_token = objects.continuation_token;
        if continuation_token.is_none() {
            break;
        }
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
        Default::default(),
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
    assert_eq!(last_checkpoint.num_slices.get(), 1);
    assert_eq!(last_checkpoint.epoch_id, epoch_id);
    assert_eq!(last_checkpoint.source_states, source_states);
    let (checkpoint_factory, handle) = CheckpointFactory::new(checkpoint, Default::default())
        .await
        .unwrap();
    (temp_dir, Arc::new(checkpoint_factory), handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    use dozer_log::tokio;

    #[tokio::test]
    async fn checkpoint_writer_should_write_records() {
        create_checkpoint_factory_for_test(&[vec![Field::Int(0)]]).await;
    }
}
