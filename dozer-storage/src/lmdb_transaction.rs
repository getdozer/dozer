use dozer_types::parking_lot::RwLockReadGuard;
use lmdb::{RoTransaction, Transaction};

use crate::{
    errors::StorageError,
    lmdb_storage::{LmdbEnvironmentManager, LmdbExclusiveTransaction, SharedTransaction},
};

pub struct ReadTransaction<'a>(RwLockReadGuard<'a, LmdbExclusiveTransaction>);

impl<'a> Transaction for ReadTransaction<'a> {
    fn txn(&self) -> *mut lmdb_sys::MDB_txn {
        self.0.txn().txn()
    }
}

/// This trait abstracts the behavior of locking a `SharedTransaction` for reading
/// and beginning a `RoTransaction` from `LmdbEnvironmentManager`.
pub trait BeginTransaction {
    type Transaction<'a>: Transaction
    where
        Self: 'a;

    fn begin_txn(&self) -> Result<Self::Transaction<'_>, StorageError>;
}

impl BeginTransaction for SharedTransaction {
    type Transaction<'a> = ReadTransaction<'a> where Self: 'a;

    fn begin_txn(&self) -> Result<Self::Transaction<'_>, StorageError> {
        Ok(ReadTransaction(self.read()))
    }
}

impl BeginTransaction for LmdbEnvironmentManager {
    type Transaction<'a> = RoTransaction<'a> where Self: 'a;

    fn begin_txn(&self) -> Result<Self::Transaction<'_>, StorageError> {
        self.begin_ro_txn()
    }
}
