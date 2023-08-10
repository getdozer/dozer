use std::sync::Arc;

use dozer_log::{
    camino::Utf8Path,
    replication::create_data_storage,
    storage::{Object, Queue, Storage},
    tokio::task::JoinHandle,
};
use dozer_types::{
    log::error, models::app_config::DataStorage, node::NodeHandle, parking_lot::Mutex,
};

use crate::{errors::ExecutionError, processor_record::ProcessorRecordStore};

#[derive(Debug)]
pub struct CheckpointFactory {
    queue: Queue,
    _storage: Box<dyn Storage>, // only used in test now
    prefix: String,
    record_store: Arc<ProcessorRecordStore>,
    state: Mutex<CheckpointWriterFactoryState>,
}

#[derive(Debug)]
pub struct CheckpointFactoryOptions {
    pub storage_config: DataStorage,
    pub persist_queue_capacity: usize,
}

impl Default for CheckpointFactoryOptions {
    fn default() -> Self {
        Self {
            storage_config: DataStorage::Local(()),
            persist_queue_capacity: 100_000,
        }
    }
}

impl CheckpointFactory {
    pub async fn new(
        options: CheckpointFactoryOptions,
        checkpoint_dir: String,
        record_store: Arc<ProcessorRecordStore>,
    ) -> Result<(Self, JoinHandle<()>), ExecutionError> {
        let (storage, prefix) = create_data_storage(options.storage_config, checkpoint_dir).await?;
        // We don't care about the worker handle here.
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
                _storage: storage,
                prefix,
                record_store,
                state,
            },
            worker,
        ))
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

        let mut object = Object::new(self.queue.clone(), key)
            .map_err(|_| ExecutionError::CheckpointWriterThreadPanicked)?;
        object
            .write(&data)
            .map_err(|_| ExecutionError::CheckpointWriterThreadPanicked)?;
        drop(object);

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

#[cfg(test)]
mod tests {
    use super::*;

    use dozer_log::tokio;
    use dozer_types::types::Field;
    use tempdir::TempDir;

    use crate::processor_record::ProcessorRecordStore;

    #[tokio::test]
    async fn checkpoint_writer_should_write_records() {
        // Simulate a recovered record store.
        let record_store = Arc::new(ProcessorRecordStore::new().unwrap());
        record_store.create_ref(&[]).unwrap();

        let temp_dir = TempDir::new("checkpoint_writer_should_write_records").unwrap();
        let (factory, join_handle) = CheckpointFactory::new(
            Default::default(),
            temp_dir.path().to_str().unwrap().to_string(),
            record_store.clone(),
        )
        .await
        .unwrap();
        let storage = dyn_clone::clone_box(&*factory._storage);

        let fields = vec![Field::Int(0)];
        record_store.create_ref(&fields).unwrap();

        // Writer must be dropped outside tokio context.
        let write_handle = std::thread::spawn(move || {
            CheckpointWriter::new(Arc::new(factory), 0);
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
