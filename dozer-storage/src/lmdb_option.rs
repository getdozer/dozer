use dozer_types::borrow::Cow;
use lmdb::{Database, RwTransaction, Transaction};

use crate::{
    errors::StorageError,
    lmdb_storage::{LmdbEnvironment, RwLmdbEnvironment},
    LmdbMap, LmdbVal,
};

#[derive(Debug)]
pub struct LmdbOption<V>(LmdbMap<u8, V>);

impl<V> Clone for LmdbOption<V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<V> Copy for LmdbOption<V> {}

impl<V: LmdbVal> LmdbOption<V> {
    pub fn create(env: &mut RwLmdbEnvironment, name: Option<&str>) -> Result<Self, StorageError> {
        let map = LmdbMap::create(env, name)?;
        Ok(Self(map))
    }

    pub fn open<E: LmdbEnvironment>(env: &E, name: Option<&str>) -> Result<Self, StorageError> {
        let map = LmdbMap::open(env, name)?;
        Ok(Self(map))
    }

    pub fn load<'txn, T: Transaction>(
        &self,
        txn: &'txn T,
    ) -> Result<Option<Cow<'txn, V>>, StorageError> {
        self.0.get(txn, &KEY)
    }

    pub fn store(&self, txn: &mut RwTransaction, value: V::Encode<'_>) -> Result<(), StorageError> {
        self.0.insert_overwrite(txn, &KEY, value)
    }

    pub fn database(&self) -> Database {
        self.0.database()
    }
}

const KEY: u8 = 0;
