use std::{num::NonZeroUsize, sync::Arc};

use dozer_log::{
    camino::{Utf8Path, Utf8PathBuf},
    dyn_clone,
    replication::create_data_storage,
    storage::{Object, Queue, Storage},
    tokio::task::JoinHandle,
};
use dozer_types::{
    log::{error, info},
    models::app_config::DataStorage,
    node::NodeHandle,
    parking_lot::Mutex,
    types::Field,
};
use tempdir::TempDir;

use crate::{errors::ExecutionError, processor_record::ProcessorRecordStore};

#[derive(Debug)]
pub struct CheckpointFactory {
    queue: Queue,
    storage: Box<dyn Storage>, // only used in test now
    prefix: String,
    record_store: Arc<ProcessorRecordStore>,
    state: Mutex<CheckpointWriterFactoryState>,
}

#[derive(Debug, Clone)]
pub struct CheckpointFactoryOptions {
    pub storage_config: DataStorage,
    pub persist_queue_capacity: usize,
}

impl Default for CheckpointFactoryOptions {
    fn default() -> Self {
        Self {
            storage_config: DataStorage::Local(()),
            persist_queue_capacity: 100,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LastCheckpoint {
    pub num_slices: NonZeroUsize,
    pub epoch_id: u64,
}

impl CheckpointFactory {
    // We need tokio runtime so mark the function as async.
    pub async fn new(
        checkpoint_dir: String,
        options: CheckpointFactoryOptions,
    ) -> Result<(Self, Option<LastCheckpoint>, JoinHandle<()>), ExecutionError> {
        let record_store = ProcessorRecordStore::new()?;

        let (storage, prefix) =
            create_data_storage(options.storage_config, checkpoint_dir.to_string()).await?;
        let last_checkpoint = read_record_store_slices(
            &*storage,
            record_store_prefix(&prefix).as_str(),
            &record_store,
        )
        .await?;
        if let Some(checkpoint) = last_checkpoint {
            info!(
                "Restored record store from {}th checkpoint, last epoch id is {}",
                checkpoint.num_slices, checkpoint.epoch_id,
            );
        }

        let (queue, worker) = Queue::new(
            dyn_clone::clone_box(&*storage),
            options.persist_queue_capacity,
        );

        let state = Mutex::new(CheckpointWriterFactoryState {
            next_record_index: record_store.num_records(),
        });

        Ok((
            Self {
                queue,
                storage,
                prefix,
                record_store: Arc::new(record_store),
                state,
            },
            last_checkpoint,
            worker,
        ))
    }

    pub fn storage(&self) -> &dyn Storage {
        &*self.storage
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn record_store(&self) -> &Arc<ProcessorRecordStore> {
        &self.record_store
    }

    fn write_record_store_slice(&self, key: String) -> Result<(), ExecutionError> {
        let mut state = self.state.lock();
        let (data, num_records_serialized) =
            self.record_store.serialize_slice(state.next_record_index)?;
        state.next_record_index += num_records_serialized;
        drop(state);

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

#[derive(Debug)]
pub struct CheckpointWriter {
    factory: Arc<CheckpointFactory>,
    record_store_key: String,
    processor_prefix: String,
}

fn record_store_prefix(factory_prefix: &str) -> Utf8PathBuf {
    AsRef::<Utf8Path>::as_ref(factory_prefix).join("record_store")
}

impl CheckpointWriter {
    pub fn new(factory: Arc<CheckpointFactory>, epoch_id: u64) -> Self {
        // Format with `u64` max number of digits.
        let epoch_id = format!("{:020}", epoch_id);
        let record_store_key = record_store_prefix(&factory.prefix)
            .join(&epoch_id)
            .into_string();
        let processor_prefix = AsRef::<Utf8Path>::as_ref(&factory.prefix)
            .join(epoch_id)
            .into_string();
        Self {
            factory,
            record_store_key,
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
        let key = AsRef::<Utf8Path>::as_ref(&self.processor_prefix)
            .join(node_handle.to_string())
            .into_string();
        Object::new(self.factory.queue.clone(), key)
            .map_err(|_| ExecutionError::CheckpointWriterThreadPanicked)
    }

    fn drop(&mut self) -> Result<(), ExecutionError> {
        self.factory
            .write_record_store_slice(std::mem::take(&mut self.record_store_key))
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
    prefix: &str,
    record_store: &ProcessorRecordStore,
) -> Result<Option<LastCheckpoint>, ExecutionError> {
    let mut last_checkpoint: Option<LastCheckpoint> = None;
    let mut continuation_token = None;
    loop {
        let objects = storage
            .list_objects(prefix.to_string(), continuation_token)
            .await?;

        if let Some(object) = objects.objects.last() {
            let object_name = AsRef::<Utf8Path>::as_ref(&object.key)
                .strip_prefix(prefix)
                .map_err(|_| ExecutionError::UnrecognizedCheckpoint(object.key.clone()))?;
            let epoch_id = object_name
                .as_str()
                .parse()
                .map_err(|_| ExecutionError::UnrecognizedCheckpoint(object.key.clone()))?;

            if let Some(last_checkpoint) = last_checkpoint.as_mut() {
                last_checkpoint
                    .num_slices
                    .checked_add(objects.objects.len())
                    .expect("shouldn't overflow");
                last_checkpoint.epoch_id = epoch_id;
            } else {
                last_checkpoint = Some(LastCheckpoint {
                    num_slices: NonZeroUsize::new(objects.objects.len())
                        .expect("have at least one element"),
                    epoch_id,
                });
            }
        }

        for object in objects.objects {
            info!("Downloading {}", object.key);
            let data = storage.download_object(object.key).await?;
            record_store.deserialize_and_extend(&data)?;
        }

        continuation_token = objects.continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    Ok(last_checkpoint)
}

/// This is only meant to be used in tests.
pub async fn create_checkpoint_factory_for_test(
    records: &[Vec<Field>],
) -> (TempDir, Arc<CheckpointFactory>, JoinHandle<()>) {
    // Create empty checkpoint storage.
    let temp_dir = TempDir::new("create_checkpoint_factory_for_test").unwrap();
    let checkpoint_dir = temp_dir.path().to_str().unwrap().to_string();
    let (checkpoint_factory, _, handle) =
        CheckpointFactory::new(checkpoint_dir.clone(), Default::default())
            .await
            .unwrap();
    let factory = Arc::new(checkpoint_factory);

    // Write data to checkpoint.
    for record in records {
        factory.record_store().create_ref(record).unwrap();
    }
    // Writer must be dropped outside tokio context.
    let epoch_id = 42;
    std::thread::spawn(move || drop(CheckpointWriter::new(factory, epoch_id)))
        .join()
        .unwrap();
    handle.await.unwrap();

    // Create a new factory that loads from the checkpoint.
    let (factory, last_checkpoint, handle) =
        CheckpointFactory::new(checkpoint_dir, Default::default())
            .await
            .unwrap();
    let last_checkpoint = last_checkpoint.unwrap();
    assert_eq!(last_checkpoint.num_slices.get(), 1);
    assert_eq!(last_checkpoint.epoch_id, epoch_id);
    assert_eq!(factory.record_store().num_records(), records.len());

    (temp_dir, Arc::new(factory), handle)
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
