use lmdb::{Database, DatabaseFlags, RwTransaction, WriteFlags};

use crate::{
    errors::StorageError,
    lmdb_map::database_key_flag,
    lmdb_storage::{LmdbEnvironmentManager, LmdbExclusiveTransaction},
    LmdbDupValue, LmdbKey, LmdbValType,
};

#[derive(Debug, Clone, Copy)]
pub struct LmdbMultimap<K: ?Sized, V: ?Sized> {
    db: Database,
    _key: std::marker::PhantomData<*const K>,
    _value: std::marker::PhantomData<*const V>,
}

impl<K: LmdbKey + ?Sized, V: LmdbDupValue + ?Sized> LmdbMultimap<K, V> {
    pub fn new_from_env(
        env: &mut LmdbEnvironmentManager,
        name: Option<&str>,
        create_if_not_exist: bool,
    ) -> Result<Self, StorageError> {
        let create_flags = if create_if_not_exist {
            Some(database_flag::<K, V>())
        } else {
            None
        };

        let db = env.create_database(name, create_flags)?;

        Ok(Self {
            db,
            _key: std::marker::PhantomData,
            _value: std::marker::PhantomData,
        })
    }

    pub fn new_from_txn(
        txn: &mut LmdbExclusiveTransaction,
        name: Option<&str>,
        create_if_not_exist: bool,
    ) -> Result<Self, StorageError> {
        let create_flags = if create_if_not_exist {
            Some(database_flag::<K, V>())
        } else {
            None
        };

        let db = txn.create_database(name, create_flags)?;

        Ok(Self {
            db,
            _key: std::marker::PhantomData,
            _value: std::marker::PhantomData,
        })
    }

    pub fn database(&self) -> Database {
        self.db
    }

    /// Returns if the key-value pair was actually inserted.
    pub fn insert(
        &self,
        txn: &mut RwTransaction,
        key: &K,
        value: &V,
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
        key: &K,
        value: &V,
    ) -> Result<bool, StorageError> {
        let key = key.encode()?;
        let value = value.encode()?;
        match txn.del(self.db, &key, Some(value.as_ref())) {
            Ok(()) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }
}

fn database_flag<K: LmdbKey + ?Sized, V: LmdbKey + ?Sized>() -> DatabaseFlags {
    let mut flags = database_key_flag::<K>();
    flags |= DatabaseFlags::DUP_SORT;
    match V::TYPE {
        LmdbValType::U32 => flags |= DatabaseFlags::DUP_FIXED | DatabaseFlags::INTEGER_DUP,
        #[cfg(target_pointer_width = "64")]
        LmdbValType::U64 => flags |= DatabaseFlags::DUP_FIXED | DatabaseFlags::INTEGER_DUP,
        LmdbValType::FixedSizeOtherThanU32OrUsize => flags |= DatabaseFlags::DUP_FIXED,
        LmdbValType::VariableSize => (),
    };
    flags
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::lmdb_storage::{LmdbEnvironmentManager, LmdbEnvironmentOptions};

    use super::*;

    #[test]
    fn test_lmdb_multimap() {
        let temp_dir = TempDir::new("test_lmdb_map").unwrap();
        let env = LmdbEnvironmentManager::create(
            temp_dir.path(),
            "env",
            LmdbEnvironmentOptions::default(),
        )
        .unwrap();
        let txn = env.create_txn().unwrap();
        let mut txn = txn.write();

        let map = LmdbMultimap::new_from_txn(&mut txn, None, true).unwrap();
        assert!(map.insert(txn.txn_mut(), &1u64, &2u64).unwrap());
        assert!(!map.insert(txn.txn_mut(), &1u64, &2u64).unwrap());
        assert!(map.insert(txn.txn_mut(), &1u64, &3u64).unwrap());
        assert!(map.remove(txn.txn_mut(), &1u64, &2u64).unwrap());
        assert!(!map.remove(txn.txn_mut(), &1u64, &2u64).unwrap());
    }
}
