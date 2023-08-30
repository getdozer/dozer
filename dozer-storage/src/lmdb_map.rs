use std::ops::Bound;

use dozer_types::borrow::Cow;
use lmdb::{Database, DatabaseFlags, RoCursor, RwTransaction, Transaction, WriteFlags};

use crate::{
    errors::StorageError,
    lmdb_storage::{LmdbEnvironment, RwLmdbEnvironment},
    Encode, Iterator, KeyIterator, LmdbKey, LmdbKeyType, LmdbVal, ValueIterator,
};

#[derive(Debug)]
pub struct LmdbMap<K, V> {
    db: Database,
    _key: std::marker::PhantomData<K>,
    _value: std::marker::PhantomData<V>,
}

impl<K, V> Clone for LmdbMap<K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K, V> Copy for LmdbMap<K, V> {}

// Safety: `Database` is `Send` and `Sync`.
unsafe impl<K, V> Send for LmdbMap<K, V> {}
unsafe impl<K, V> Sync for LmdbMap<K, V> {}

impl<K: LmdbKey, V: LmdbVal> LmdbMap<K, V> {
    pub fn create(env: &mut RwLmdbEnvironment, name: Option<&str>) -> Result<Self, StorageError> {
        let db = env.create_database(name, database_key_flag::<K>())?;

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

    pub fn count<T: Transaction>(&self, txn: &T) -> Result<usize, StorageError> {
        Ok(txn.stat(self.db)?.entries())
    }

    pub fn get<'a, T: Transaction>(
        &self,
        txn: &'a T,
        key: K::Encode<'_>,
    ) -> Result<Option<Cow<'a, V>>, StorageError> {
        let key = key.encode()?;
        match txn.get(self.db, &key) {
            Ok(value) => Ok(Some(V::decode(value)?)),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Returns if the key was actually inserted.
    pub fn insert(
        &self,
        txn: &mut RwTransaction,
        key: K::Encode<'_>,
        value: V::Encode<'_>,
    ) -> Result<bool, StorageError> {
        let key = key.encode()?;
        let value = value.encode()?;
        match txn.put(self.db, &key, &value, WriteFlags::NO_OVERWRITE) {
            Ok(()) => Ok(true),
            Err(lmdb::Error::KeyExist) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Inserts or overwrites the value.
    pub fn insert_overwrite(
        &self,
        txn: &mut RwTransaction,
        key: K::Encode<'_>,
        value: V::Encode<'_>,
    ) -> Result<(), StorageError> {
        let key = key.encode()?;
        let value = value.encode()?;
        txn.put(self.db, &key, &value, WriteFlags::empty())?;
        Ok(())
    }

    /// User must ensure that the key is larger than any existing key.
    pub fn append(
        &self,
        txn: &mut RwTransaction,
        key: K::Encode<'_>,
        value: V::Encode<'_>,
    ) -> Result<(), StorageError> {
        let key = key.encode()?;
        let data = value.encode()?;
        txn.put(self.db, &key, &data, WriteFlags::APPEND)
            .map_err(Into::into)
    }

    /// Returns if the key was actually removed.
    pub fn remove(
        &self,
        txn: &mut RwTransaction,
        key: K::Encode<'_>,
    ) -> Result<bool, StorageError> {
        let key = key.encode()?;
        match txn.del(self.db, &key, None) {
            Ok(()) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub fn clear(&self, txn: &mut RwTransaction) -> Result<(), StorageError> {
        txn.clear_db(self.db).map_err(Into::into)
    }

    pub fn iter<'txn, T: Transaction>(
        &self,
        txn: &'txn T,
    ) -> Result<Iterator<'txn, RoCursor<'txn>, K, V>, StorageError> {
        let cursor = txn.open_ro_cursor(self.db)?;
        Iterator::new(cursor, Bound::Unbounded, true)
    }

    pub fn keys<'txn, T: Transaction>(
        &self,
        txn: &'txn T,
    ) -> Result<KeyIterator<'txn, RoCursor<'txn>, K>, StorageError> {
        let cursor = txn.open_ro_cursor(self.db)?;
        KeyIterator::new(cursor, Bound::Unbounded, true)
    }

    pub fn values<'txn, T: Transaction>(
        &self,
        txn: &'txn T,
    ) -> Result<ValueIterator<'txn, RoCursor<'txn>, V>, StorageError> {
        let cursor = txn.open_ro_cursor(self.db)?;
        ValueIterator::new::<K>(cursor, Bound::Unbounded, true)
    }
}

impl<'a, K: LmdbKey + 'a, V: LmdbVal + 'a> LmdbMap<K, V> {
    /// Extend the map with the contents of an iterator.
    ///
    /// Keys that exist in the map before insertion are ignored.
    pub fn extend(
        &self,
        txn: &mut RwTransaction,
        iter: impl IntoIterator<Item = (K::Encode<'a>, V::Encode<'a>)>,
    ) -> Result<(), StorageError> {
        for (key, value) in iter {
            self.insert(txn, key, value)?;
        }
        Ok(())
    }
}

pub fn database_key_flag<K: LmdbKey>() -> DatabaseFlags {
    match K::TYPE {
        LmdbKeyType::U32 => DatabaseFlags::INTEGER_KEY,
        #[cfg(target_pointer_width = "64")]
        LmdbKeyType::U64 => DatabaseFlags::INTEGER_KEY,
        LmdbKeyType::FixedSizeOtherThanU32OrUsize | LmdbKeyType::VariableSize => {
            DatabaseFlags::empty()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::lmdb_storage::{LmdbEnvironmentManager, LmdbEnvironmentOptions};

    use super::*;

    use dozer_types::borrow::IntoOwned;
    use tempdir::TempDir;

    #[test]
    fn test_lmdb_map() {
        let temp_dir = TempDir::new("test_lmdb_map").unwrap();
        let mut env = LmdbEnvironmentManager::create_rw(
            temp_dir.path(),
            "env",
            LmdbEnvironmentOptions::default(),
        )
        .unwrap();
        let map = LmdbMap::<Vec<u8>, Vec<u8>>::create(&mut env, None).unwrap();

        assert_eq!(map.count(env.txn_mut().unwrap()).unwrap(), 0);

        assert!(map
            .insert(env.txn_mut().unwrap(), [1u8].as_slice(), [2u8].as_slice())
            .unwrap());
        assert_eq!(map.count(env.txn_mut().unwrap()).unwrap(), 1);

        assert!(!map
            .insert(env.txn_mut().unwrap(), [1u8].as_slice(), [3u8].as_slice())
            .unwrap());
        assert_eq!(map.count(env.txn_mut().unwrap()).unwrap(), 1);

        assert_eq!(
            map.get(env.txn_mut().unwrap(), [1u8].as_slice())
                .unwrap()
                .unwrap()
                .into_owned(),
            vec![2]
        );
        assert!(map
            .get(env.txn_mut().unwrap(), [2u8].as_slice())
            .unwrap()
            .is_none());

        assert!(!map
            .remove(env.txn_mut().unwrap(), [2u8].as_slice())
            .unwrap());
        assert_eq!(map.count(env.txn_mut().unwrap()).unwrap(), 1);
        assert!(map
            .remove(env.txn_mut().unwrap(), [1u8].as_slice())
            .unwrap());
        assert_eq!(map.count(env.txn_mut().unwrap()).unwrap(), 0);
    }

    #[test]
    fn test_lmdb_map_append() {
        let temp_dir = TempDir::new("test_lmdb_map_append").unwrap();
        let mut env = LmdbEnvironmentManager::create_rw(
            temp_dir.path(),
            "env",
            LmdbEnvironmentOptions::default(),
        )
        .unwrap();
        let map = LmdbMap::<u64, u64>::create(&mut env, None).unwrap();

        let txn = env.txn_mut().unwrap();
        for i in 0..=256u64 {
            assert_eq!(map.count(txn).unwrap() as u64, i);
            map.append(txn, &i, &i).unwrap();
            assert_eq!(map.count(txn).unwrap() as u64, i + 1);
            for j in 0..=i {
                assert_eq!(map.get(txn, &j).unwrap().unwrap().into_owned(), j);
            }
            assert_eq!(map.get(txn, &(i + 1)).unwrap(), None);
        }
    }

    #[test]
    fn test_lmdb_map_illegal_append() {
        let temp_dir = TempDir::new("test_lmdb_map_illegal_append").unwrap();
        let mut env = LmdbEnvironmentManager::create_rw(
            temp_dir.path(),
            "env",
            LmdbEnvironmentOptions::default(),
        )
        .unwrap();
        let map = LmdbMap::<u64, u64>::create(&mut env, None).unwrap();

        let txn = env.txn_mut().unwrap();
        map.append(txn, &1, &1).unwrap();
        assert!(matches!(
            map.append(txn, &1, &2).unwrap_err(),
            StorageError::Lmdb(lmdb::Error::KeyExist)
        ));
        assert!(matches!(
            map.append(txn, &0, &0).unwrap_err(),
            StorageError::Lmdb(lmdb::Error::KeyExist)
        ));
    }
}
