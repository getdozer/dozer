#![allow(clippy::type_complexity)]
use lmdb_sys::MDB_cmp_func;

use crate::storage::errors::StorageError;

pub struct Database {
    pub id: usize,
}

impl Database {
    pub fn new(id: usize) -> Self {
        Self { id }
    }
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Database { id: self.id }
    }
}

unsafe impl Send for Database {}
unsafe impl Sync for Database {}

pub trait EnvironmentManager: Environment {
    fn as_environment(&mut self) -> &mut dyn Environment;
    fn create_txn(&mut self) -> Result<Box<dyn RenewableRwTransaction>, StorageError>;
}

pub trait Environment {
    fn open_database(
        &mut self,
        name: &str,
        dup_keys: bool,
        comparator: MDB_cmp_func,
    ) -> Result<Database, StorageError>;
}

pub trait RenewableRwTransaction: Send + Sync {
    fn commit_and_renew(&mut self) -> Result<(), StorageError>;
    fn abort_and_renew(&mut self) -> Result<(), StorageError>;
    fn put(&mut self, db: &Database, key: &[u8], value: &[u8]) -> Result<(), StorageError>;
    fn del(
        &mut self,
        db: &Database,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<bool, StorageError>;
    fn open_cursor(&self, db: &Database) -> Result<Box<dyn RwCursor>, StorageError>;
    fn get(&self, db: &Database, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
    fn open_ro_cursor(&self, db: &Database) -> Result<Box<dyn RoCursor>, StorageError>;
}

pub trait RoTransaction {
    fn get(&self, db: &Database, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
    fn open_cursor(&self, db: &Database) -> Result<Box<dyn RoCursor>, StorageError>;
}

pub trait RwTransaction {
    fn get(&self, db: &Database, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
    fn put(&mut self, db: &Database, key: &[u8], value: &[u8]) -> Result<(), StorageError>;
    fn del(
        &mut self,
        db: &Database,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<bool, StorageError>;
    fn open_cursor(&self, db: &Database) -> Result<Box<dyn RwCursor>, StorageError>;
}

pub trait RoCursor {
    fn seek_gte(&self, key: &[u8]) -> Result<bool, StorageError>;
    fn seek(&self, key: &[u8]) -> Result<bool, StorageError>;
    fn seek_partial(&self, key: &[u8]) -> Result<bool, StorageError>;
    fn read(&self) -> Result<Option<(&[u8], &[u8])>, StorageError>;
    fn next(&self) -> Result<bool, StorageError>;
    fn prev(&self) -> Result<bool, StorageError>;
    fn first(&self) -> Result<bool, StorageError>;
    fn last(&self) -> Result<bool, StorageError>;
}

pub trait RwCursor: RoCursor {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;
}
