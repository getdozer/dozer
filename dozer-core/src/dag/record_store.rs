use crate::storage::common::{Database, RenewableRwTransaction, RoTransaction, RwTransaction};
use crate::storage::errors::StorageError;
use ahash::HashMap;
use std::sync::{Arc, RwLock};

pub struct RecordReader {
    tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
    db: Database,
}

impl RecordReader {
    pub fn new(tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>, db: Database) -> Self {
        Self { tx, db }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.tx.read().unwrap().get(&self.db, key)
        //Ok(None)
    }
}

unsafe impl Send for RecordReader {}
unsafe impl Sync for RecordReader {}
