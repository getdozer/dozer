use dozer_types::borrow::Cow;
use lmdb::{RwTransaction, Transaction};

use crate::{errors::StorageError, lmdb_storage::CreateDatabase, LmdbMap, LmdbVal};

#[derive(Debug)]
pub struct LmdbOption<V>(LmdbMap<u8, V>);

impl<V> Clone for LmdbOption<V> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl<V> Copy for LmdbOption<V> {}

impl<V: LmdbVal> LmdbOption<V> {
    pub fn new<C: CreateDatabase>(
        c: &mut C,
        name: Option<&str>,
        create_if_not_exist: bool,
    ) -> Result<Self, StorageError> {
        let map = LmdbMap::new(c, name, create_if_not_exist)?;
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
}

const KEY: u8 = 0;
