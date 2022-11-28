use crate::storage::common::{Database, RenewableRwTransaction};
use crate::storage::errors::StorageError;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::Schema;
use std::sync::Arc;

pub struct RecordStore {
    tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
    db: Database,
    schema: Schema,
}

impl RecordStore {
    pub fn new(
        tx: Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
        db: Database,
        schema: Schema,
    ) -> Self {
        Self { tx, db, schema }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.tx.read().get(&self.db, key)
    }
}

unsafe impl Send for RecordStore {}
unsafe impl Sync for RecordStore {}
