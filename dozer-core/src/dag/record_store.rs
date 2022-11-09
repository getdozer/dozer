use crate::storage::common::{Database, RenewableRwTransaction};
use crate::storage::errors::StorageError;
use ahash::HashMap;
use dozer_types::parking_lot::RwLock;
use std::sync::Arc;

pub struct RecordReader {
    tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
    db: Database,
}

impl RecordReader {
    pub fn new(tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>, db: Database) -> Self {
        Self { tx, db }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.tx.read().get(&self.db, key)
    }
}

unsafe impl Send for RecordReader {}
unsafe impl Sync for RecordReader {}
