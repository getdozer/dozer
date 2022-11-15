use crate::storage::common::{
    Database, RenewableRwTransaction, RoCursor, RoTransaction, RwCursor, RwTransaction,
};
use crate::storage::errors::StorageError;
use dozer_types::parking_lot::RwLock;
use std::sync::Arc;

pub struct SharedTransaction<'a> {
    tx: &'a Arc<RwLock<Box<dyn RenewableRwTransaction>>>,
}

impl<'a> SharedTransaction<'a> {
    pub fn new(tx: &'a Arc<RwLock<Box<dyn RenewableRwTransaction>>>) -> Self {
        Self { tx }
    }
}

impl<'a> RwTransaction for SharedTransaction<'a> {
    #[inline]
    fn get(&self, db: &Database, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.tx.read().get(db, key)
    }

    #[inline]
    fn put(&mut self, db: &Database, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.tx.write().put(db, key, value)
    }

    #[inline]
    fn del(
        &mut self,
        db: &Database,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<bool, StorageError> {
        self.tx.write().del(db, key, value)
    }

    fn open_cursor(&self, db: &Database) -> Result<Box<dyn RwCursor>, StorageError> {
        self.tx.read().open_cursor(db)
    }
}

impl<'a> RoTransaction for SharedTransaction<'a> {
    #[inline]
    fn get(&self, db: &Database, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.tx.read().get(db, key)
    }

    #[inline]
    fn open_cursor(&self, db: &Database) -> Result<Box<dyn RoCursor>, StorageError> {
        self.tx.read().open_ro_cursor(db)
    }
}

pub struct ExclusiveTransaction<'a> {
    tx: &'a mut Box<dyn RenewableRwTransaction>,
}

impl<'a> ExclusiveTransaction<'a> {
    pub fn new(tx: &'a mut Box<dyn RenewableRwTransaction>) -> Self {
        Self { tx }
    }
}

impl<'a> RwTransaction for ExclusiveTransaction<'a> {
    #[inline]
    fn get(&self, db: &Database, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.tx.get(db, key)
    }

    #[inline]
    fn put(&mut self, db: &Database, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.tx.put(db, key, value)
    }

    #[inline]
    fn del(
        &mut self,
        db: &Database,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<bool, StorageError> {
        self.tx.del(db, key, value)
    }

    fn open_cursor(&self, db: &Database) -> Result<Box<dyn RwCursor>, StorageError> {
        self.tx.open_cursor(db)
    }
}

impl<'a> RoTransaction for ExclusiveTransaction<'a> {
    #[inline]
    fn get(&self, db: &Database, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.tx.get(db, key)
    }

    fn open_cursor(&self, db: &Database) -> Result<Box<dyn RoCursor>, StorageError> {
        self.tx.open_ro_cursor(db)
    }
}
