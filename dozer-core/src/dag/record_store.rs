use crate::storage::common::{Database, RenewableRwTransaction, RoTransaction, RwTransaction};
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
}

unsafe impl Send for RecordReader {}
unsafe impl Sync for RecordReader {}
