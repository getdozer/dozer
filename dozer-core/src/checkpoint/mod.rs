use std::sync::Arc;

use dozer_log::{
    camino::Utf8Path,
    dyn_clone,
    replication::create_data_storage,
    storage::{Object, Queue, Storage},
    tokio::task::JoinHandle,
};
use dozer_types::{
    log::error, models::app_config::DataStorage, node::NodeHandle, parking_lot::Mutex, types::Field,
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

impl CheckpointFactory {
    // We need tokio runtime so mark the function as async.
    pub async fn new(
        record_store: Arc<ProcessorRecordStore>,
        checkpoint_dir: String,
        options: CheckpointFactoryOptions,
    ) -> Result<(Self, JoinHandle<()>), ExecutionError> {
        let (storage, prefix) =
            create_data_storage(options.storage_config, checkpoint_dir.to_string()).await?;

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
                record_store,
                state,
            },
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

const RECORD_STORE_DIR_NAME: &str = "record_store";

impl CheckpointWriter {
    pub fn new(factory: Arc<CheckpointFactory>, epoch_id: u64) -> Self {
        let prefix: &Utf8Path = factory.prefix.as_ref();
        // Format with `u64` max number of digits.
        let epoch_id = format!("{:020}", epoch_id);
        let record_store_key = prefix
            .join(RECORD_STORE_DIR_NAME)
            .join(&epoch_id)
            .into_string();
        let processor_prefix = prefix.join(epoch_id).into_string();
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

/// This is only meant to be used in tests.
pub async fn create_checkpoint_factory_for_test(
    records: &[Vec<Field>],
) -> (TempDir, Arc<CheckpointFactory>, JoinHandle<()>) {
    let record_store = Arc::new(ProcessorRecordStore::new().unwrap());
    for record in records {
        record_store.create_ref(record).unwrap();
    }

    let temp_dir = TempDir::new("create_checkpoint_factory_for_test").unwrap();
    let (checkpoint_factory, handle) = CheckpointFactory::new(
        record_store,
        temp_dir.path().to_str().unwrap().to_string(),
        Default::default(),
    )
    .await
    .unwrap();
    (temp_dir, Arc::new(checkpoint_factory), handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    use dozer_log::tokio;
    use dozer_types::types::Field;

    #[tokio::test]
    async fn checkpoint_writer_should_write_records() {
        let (_temp_dir, factory, join_handle) = create_checkpoint_factory_for_test(&[vec![]]).await;
        let storage = dyn_clone::clone_box(factory.storage());

        let fields = vec![Field::Int(0)];
        factory.record_store().create_ref(&fields).unwrap();

        // Writer must be dropped outside tokio context.
        let write_handle = std::thread::spawn(move || {
            CheckpointWriter::new(factory, 0);
        });
        join_handle.await.unwrap();
        write_handle.join().unwrap();

        // We only assert something is written to the storage for now. Will check if data can be properly restored later.
        assert_eq!(
            storage
                .list_objects("".to_string(), None)
                .await
                .unwrap()
                .objects
                .len(),
            1
        );
    }
}
