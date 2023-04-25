use std::ops::Bound;

use dozer_types::borrow::Cow;
use lmdb::{Cursor, Database, DatabaseFlags, RoCursor, RwTransaction, Transaction, WriteFlags};
use lmdb_sys::{MDB_LAST_DUP, MDB_SET};

use crate::{
    errors::StorageError,
    lmdb_map::database_key_flag,
    lmdb_storage::{LmdbEnvironment, RwLmdbEnvironment},
    Encode, Iterator, LmdbKey, LmdbKeyType,
};

#[derive(Debug)]
pub struct LmdbMultimap<K, V> {
    db: Database,
    _key: std::marker::PhantomData<*const K>,
    _value: std::marker::PhantomData<*const V>,
}

impl<K, V> Clone for LmdbMultimap<K, V> {
    fn clone(&self) -> Self {
        Self {
            db: self.db,
            _key: std::marker::PhantomData,
            _value: std::marker::PhantomData,
        }
    }
}

impl<K, V> Copy for LmdbMultimap<K, V> {}

// Safety: `Database` is `Send` and `Sync`.
unsafe impl<K, V> Send for LmdbMultimap<K, V> {}
unsafe impl<K, V> Sync for LmdbMultimap<K, V> {}

impl<K: LmdbKey, V: LmdbKey> LmdbMultimap<K, V> {
    pub fn create(env: &mut RwLmdbEnvironment, name: Option<&str>) -> Result<Self, StorageError> {
        let db = env.create_database(name, database_flag::<K, V>())?;

        Ok(Self {
            db,
            _key: std::marker::PhantomData,
            _value: std::marker::PhantomData,
        })
    }

    pub fn open<E: LmdbEnvironment>(env: &E, name: Option<&str>) -> Result<Self, StorageError> {
        let db = env.open_database(name)?;

        Ok(Self {
            db,
            _key: std::marker::PhantomData,
            _value: std::marker::PhantomData,
        })
    }

    pub fn database(&self) -> Database {
        self.db
    }

    pub fn count_data<T: Transaction>(&self, txn: &T) -> Result<usize, StorageError> {
        Ok(txn.stat(self.db)?.entries())
    }

    pub fn get_first<'a, T: Transaction>(
        &self,
        txn: &'a T,
        key: K::Encode<'_>,
    ) -> Result<Option<Cow<'a, V>>, StorageError> {
        self.get(txn, key, true)
    }

    pub fn get_last<'a, T: Transaction>(
        &self,
        txn: &'a T,
        key: K::Encode<'_>,
    ) -> Result<Option<Cow<'a, V>>, StorageError> {
        self.get(txn, key, false)
    }

    fn get<'a, T: Transaction>(
        &self,
        txn: &'a T,
        key: K::Encode<'_>,
        first: bool,
    ) -> Result<Option<Cow<'a, V>>, StorageError> {
        let key = key.encode()?;
        let cursor = txn.open_ro_cursor(self.db)?;

        match cursor.get(Some(key.as_ref()), None, MDB_SET) {
            Ok((_, value)) => {
                if first {
                    Ok(Some(V::decode(value)?))
                } else {
                    let (_, value) = cursor.get(None, None, MDB_LAST_DUP)?;
                    Ok(Some(V::decode(value)?))
                }
            }
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Returns if the key-value pair was actually inserted.
    pub fn insert(
        &self,
        txn: &mut RwTransaction,
        key: K::Encode<'_>,
        value: V::Encode<'_>,
    ) -> Result<bool, StorageError> {
        let key = key.encode()?;
        let value = value.encode()?;
        match txn.put(self.db, &key, &value, WriteFlags::NO_DUP_DATA) {
            Ok(()) => Ok(true),
            Err(lmdb::Error::KeyExist) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    /// Returns if the key-value pair was actually removed.
    pub fn remove(
        &self,
        txn: &mut RwTransaction,
        key: K::Encode<'_>,
        value: V::Encode<'_>,
    ) -> Result<bool, StorageError> {
        let key = key.encode()?;
        let value = value.encode()?;
        match txn.del(self.db, &key, Some(value.as_ref())) {
            Ok(()) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    pub fn iter<'txn, T: Transaction>(
        &self,
        txn: &'txn T,
    ) -> Result<Iterator<'txn, RoCursor<'txn>, K, V>, StorageError> {
        let cursor = txn.open_ro_cursor(self.db)?;
        Iterator::new(cursor, Bound::Unbounded, true)
    }

    pub fn range<'txn, T: Transaction>(
        &self,
        txn: &'txn T,
        starting_key: Bound<K::Encode<'_>>,
        ascending: bool,
    ) -> Result<Iterator<'txn, RoCursor<'txn>, K, V>, StorageError> {
        let cursor = txn.open_ro_cursor(self.db)?;
        Iterator::new(cursor, starting_key, ascending)
    }
}

fn database_flag<K: LmdbKey, V: LmdbKey>() -> DatabaseFlags {
    let mut flags = database_key_flag::<K>();
    flags |= DatabaseFlags::DUP_SORT;
    match V::TYPE {
        LmdbKeyType::U32 => flags |= DatabaseFlags::DUP_FIXED | DatabaseFlags::INTEGER_DUP,
        #[cfg(target_pointer_width = "64")]
        LmdbKeyType::U64 => flags |= DatabaseFlags::DUP_FIXED | DatabaseFlags::INTEGER_DUP,
        LmdbKeyType::FixedSizeOtherThanU32OrUsize => flags |= DatabaseFlags::DUP_FIXED,
        LmdbKeyType::VariableSize => (),
    };
    flags
}

#[cfg(test)]
mod tests {
    use dozer_types::borrow::IntoOwned;
    use tempdir::TempDir;

    use crate::lmdb_storage::{LmdbEnvironmentManager, LmdbEnvironmentOptions};

    use super::*;

    #[test]
    fn test_lmdb_multimap() {
        let temp_dir = TempDir::new("test_lmdb_map").unwrap();
        let mut env = LmdbEnvironmentManager::create_rw(
            temp_dir.path(),
            "env",
            LmdbEnvironmentOptions::default(),
        )
        .unwrap();
        let map = LmdbMultimap::<u64, u64>::create(&mut env, None).unwrap();

        let txn = env.txn_mut().unwrap();
        assert!(map.get_first(txn, &0).unwrap().is_none());
        assert!(map.get_last(txn, &0).unwrap().is_none());
        assert!(map.insert(txn, &1u64, &2u64).unwrap());
        assert!(!map.insert(txn, &1u64, &2u64).unwrap());
        assert!(map.insert(txn, &1u64, &3u64).unwrap());
        assert!(map.get_first(txn, &0).unwrap().is_none());
        assert!(map.get_last(txn, &0).unwrap().is_none());
        assert_eq!(map.get_first(txn, &1).unwrap().unwrap().into_owned(), 2);
        assert_eq!(map.get_last(txn, &1).unwrap().unwrap().into_owned(), 3);
        assert!(map.remove(txn, &1u64, &2u64).unwrap());
        assert!(!map.remove(txn, &1u64, &2u64).unwrap());
    }
}
