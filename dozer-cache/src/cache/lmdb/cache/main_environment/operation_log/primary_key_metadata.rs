use dozer_storage::{
    errors::StorageError,
    lmdb::{Database, Transaction},
    LmdbEnvironment, LmdbMap, RwLmdbEnvironment,
};
use dozer_types::borrow::IntoOwned;

use super::{metadata::Metadata, RecordMetadata};

#[derive(Debug, Clone, Copy)]
pub struct PrimaryKeyMetadata(LmdbMap<Vec<u8>, RecordMetadata>);

impl PrimaryKeyMetadata {
    pub fn create(env: &mut RwLmdbEnvironment) -> Result<Self, StorageError> {
        LmdbMap::create(env, Some(Self::DATABASE_NAME)).map(Self)
    }

    pub fn open<E: LmdbEnvironment>(env: &E) -> Result<Self, StorageError> {
        LmdbMap::open(env, Some(Self::DATABASE_NAME)).map(Self)
    }

    fn get<T: Transaction>(
        &self,
        txn: &T,
        key: &[u8],
    ) -> Result<Option<RecordMetadata>, StorageError> {
        self.0
            .get(txn, key)
            .map(|metadata| metadata.map(|metadata| metadata.into_owned()))
    }

    pub const DATABASE_NAME: &'static str = "primary_key_metadata";

    pub fn database(&self) -> Database {
        self.0.database()
    }
}

impl Metadata for PrimaryKeyMetadata {
    type Key<'a> = &'a [u8];

    fn count_data<T: Transaction>(&self, txn: &T) -> Result<usize, StorageError> {
        self.0.count(txn)
    }

    fn get_present<T: Transaction>(
        &self,
        txn: &T,
        key: &[u8],
    ) -> Result<Option<RecordMetadata>, StorageError> {
        self.get(txn, key)
    }

    fn get_deleted<T: Transaction>(
        &self,
        txn: &T,
        key: &[u8],
    ) -> Result<Option<RecordMetadata>, StorageError> {
        self.get(txn, key)
    }

    fn insert(
        &self,
        txn: &mut dozer_storage::lmdb::RwTransaction,
        key: &[u8],
        value: &RecordMetadata,
    ) -> Result<(), StorageError> {
        let inserted = self.0.insert(txn, key, value)?;
        debug_assert!(inserted);
        Ok(())
    }

    fn insert_overwrite(
        &self,
        txn: &mut dozer_storage::lmdb::RwTransaction,
        key: &[u8],
        _old: &RecordMetadata,
        new: &RecordMetadata,
    ) -> Result<(), StorageError> {
        self.0.insert_overwrite(txn, key, new)
    }
}
