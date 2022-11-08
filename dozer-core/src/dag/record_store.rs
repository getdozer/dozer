use crate::storage::common::{Database, RenewableRwTransaction, RoTransaction, RwTransaction};

pub struct RecordReader<'a> {
    tx: &'a dyn RoTransaction,
    db: &'a Database,
}

impl<'a> RecordReader<'a> {
    pub fn new(tx: &'a dyn RoTransaction, db: &'a Database) -> Self {
        Self { tx, db }
    }
}

unsafe impl<'a> Send for RecordReader<'a> {}
unsafe impl<'a> Sync for RecordReader<'a> {}
