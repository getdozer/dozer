pub use lmdb::{Cursor, Database, RwTransaction, Transaction};
use lmdb_sys::{MDB_GET_CURRENT, MDB_SET, MDB_SET_RANGE};

use crate::errors::StorageError;

pub trait Seek<'txn>: Cursor<'txn> {
    fn seek(&self, key: &[u8]) -> Result<bool, StorageError> {
        match self.get(Some(key), None, MDB_SET) {
            Ok(_) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn seek_gte(&self, key: &[u8]) -> Result<bool, StorageError> {
        match self.get(Some(key), None, MDB_SET_RANGE) {
            Ok(_) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    #[allow(clippy::type_complexity)]
    fn read(&'txn self) -> Result<Option<(&[u8], &[u8])>, StorageError> {
        match self.get(None, None, MDB_GET_CURRENT) {
            Ok((key, value)) => Ok(Some((
                key.expect("MDB_GET_CURRENT should always return some data when found"),
                value,
            ))),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn next(&self) -> Result<bool, StorageError> {
        match self.get(None, None, lmdb_sys::MDB_NEXT) {
            Ok(_) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn prev(&self) -> Result<bool, StorageError> {
        match self.get(None, None, lmdb_sys::MDB_PREV) {
            Ok(_) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn first(&self) -> Result<bool, StorageError> {
        match self.get(None, None, lmdb_sys::MDB_FIRST) {
            Ok(_) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn last(&self) -> Result<bool, StorageError> {
        match self.get(None, None, lmdb_sys::MDB_LAST) {
            Ok(_) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

impl<'txn, C: Cursor<'txn>> Seek<'txn> for C {}
