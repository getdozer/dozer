use std::sync::atomic::AtomicU64;

use dozer_storage::RocksdbMap;
use dozer_types::models::app_config::RocksdbConfig;
use dozer_types::types::Field;
use tempdir::TempDir;

use crate::RecordStoreError;

#[derive(Debug)]
pub struct ProcessorRecordStore {
    _temp_dir: TempDir,
    next_id: AtomicU64,
    records: RocksdbMap<u64, Vec<Field>>,
}

impl ProcessorRecordStore {
    pub fn new(config: RocksdbConfig) -> Result<Self, RecordStoreError> {
        let temp_dir = TempDir::new("rocksdb_processor_record_store")
            .map_err(RecordStoreError::FailedToCreateTempDir)?;
        let records = RocksdbMap::<u64, Vec<Field>>::create(temp_dir.path(), config)?;

        Ok(Self {
            _temp_dir: temp_dir,
            next_id: AtomicU64::new(0),
            records,
        })
    }

    pub fn num_records(&self) -> usize {
        self.next_id.load(std::sync::atomic::Ordering::SeqCst) as usize
    }

    pub fn create_ref(&self, values: &[Field]) -> Result<u64, RecordStoreError> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.records.insert(&id, values)?;
        Ok(id)
    }

    pub fn load_ref(&self, record_ref: &u64) -> Result<Vec<Field>, RecordStoreError> {
        self.records
            .get(record_ref)?
            .ok_or(RecordStoreError::RocksdbRecordNotFound(*record_ref))
    }

    pub fn serialize_slice(&self, _start: usize) -> Result<(Vec<u8>, usize), RecordStoreError> {
        todo!("implement rocksdb record store checkpointing")
    }

    pub fn deserialize_and_extend(&self, _data: &[u8]) -> Result<(), RecordStoreError> {
        todo!("implement rocksdb record store checkpointing")
    }
}
