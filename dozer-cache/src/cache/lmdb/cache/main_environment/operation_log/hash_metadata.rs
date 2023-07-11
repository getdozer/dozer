use dozer_storage::{
    errors::StorageError,
    lmdb::{Database, RwTransaction, Transaction},
    LmdbEnvironment, LmdbMultimap, RwLmdbEnvironment,
};
use dozer_types::{borrow::IntoOwned, types::Record};

use super::{metadata::Metadata, RecordMetadata};

#[derive(Debug, Clone, Copy)]
pub struct HashMetadata(LmdbMultimap<u64, RecordMetadata>);

impl HashMetadata {
    pub fn create(env: &mut RwLmdbEnvironment) -> Result<Self, StorageError> {
        LmdbMultimap::create(env, Some(Self::DATABASE_NAME)).map(Self)
    }

    pub fn open<E: LmdbEnvironment>(env: &E) -> Result<Self, StorageError> {
        LmdbMultimap::open(env, Some(Self::DATABASE_NAME)).map(Self)
    }

    pub const DATABASE_NAME: &str = "hash_metadata";

    pub fn database(&self) -> Database {
        self.0.database()
    }
}

impl Metadata for HashMetadata {
    type Key<'a> = (&'a Record, u64);

    fn count_data<T: Transaction>(&self, txn: &T) -> Result<usize, StorageError> {
        self.0.count_data(txn)
    }

    fn get_present<T: Transaction>(
        &self,
        txn: &T,
        key: (&Record, u64),
    ) -> Result<Option<RecordMetadata>, StorageError> {
        self.0
            .get_last(txn, &key.1)
            .map(|metadata| metadata.map(|metadata| metadata.into_owned()))
    }

    fn get_deleted<T: Transaction>(
        &self,
        txn: &T,
        key: (&Record, u64),
    ) -> Result<Option<RecordMetadata>, StorageError> {
        self.0
            .get_first(txn, &key.1)
            .map(|metadata| metadata.map(|metadata| metadata.into_owned()))
    }

    fn insert(
        &self,
        txn: &mut RwTransaction,
        key: (&Record, u64),
        value: &RecordMetadata,
    ) -> Result<(), StorageError> {
        let inserted = self.0.insert(txn, &key.1, value)?;
        debug_assert!(inserted);
        Ok(())
    }

    fn insert_overwrite(
        &self,
        txn: &mut RwTransaction,
        key: (&Record, u64),
        old: &RecordMetadata,
        new: &RecordMetadata,
    ) -> Result<(), StorageError> {
        let removed = self.0.remove(txn, &key.1, old)?;
        debug_assert!(removed);
        let inserted = self.0.insert(txn, &key.1, new)?;
        debug_assert!(inserted);
        Ok(())
    }
}
