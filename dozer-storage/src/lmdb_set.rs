use lmdb::{Database, RoCursor, RwTransaction, Transaction};

use crate::{
    errors::StorageError,
    lmdb_storage::{LmdbEnvironment, RwLmdbEnvironment},
    KeyIterator, LmdbKey, LmdbMap,
};

#[derive(Debug)]
pub struct LmdbSet<K>(LmdbMap<K, Vec<u8>>);

impl<K> Clone for LmdbSet<K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K> Copy for LmdbSet<K> {}

impl<K: LmdbKey> LmdbSet<K> {
    pub fn create(env: &mut RwLmdbEnvironment, name: Option<&str>) -> Result<Self, StorageError> {
        LmdbMap::create(env, name).map(Self)
    }

    pub fn open<E: LmdbEnvironment>(env: &E, name: Option<&str>) -> Result<Self, StorageError> {
        LmdbMap::open(env, name).map(Self)
    }

    pub fn count<T: Transaction>(&self, txn: &T) -> Result<usize, StorageError> {
        self.0.count(txn)
    }

    pub fn contains<T: Transaction>(
        &self,
        txn: &T,
        key: K::Encode<'_>,
    ) -> Result<bool, StorageError> {
        self.0.get(txn, key).map(|value| value.is_some())
    }

    /// Returns if the key was actually inserted.
    pub fn insert(
        &self,
        txn: &mut RwTransaction,
        key: K::Encode<'_>,
    ) -> Result<bool, StorageError> {
        self.0.insert(txn, key, &[])
    }

    /// Returns if the key was actually removed.
    pub fn remove(
        &self,
        txn: &mut RwTransaction,
        key: K::Encode<'_>,
    ) -> Result<bool, StorageError> {
        self.0.remove(txn, key)
    }

    pub fn clear(&self, txn: &mut RwTransaction) -> Result<(), StorageError> {
        self.0.clear(txn)
    }

    pub fn iter<'txn, T: Transaction>(
        &self,
        txn: &'txn T,
    ) -> Result<KeyIterator<'txn, RoCursor<'txn>, K>, StorageError> {
        self.0.keys(txn)
    }

    pub fn database(&self) -> Database {
        self.0.database()
    }
}

impl<'a, K: LmdbKey + 'a> LmdbSet<K> {
    /// Extend the set with the contents of an iterator.
    ///
    /// Keys that exist in the map before insertion are ignored.
    pub fn extend(
        &self,
        txn: &mut RwTransaction,
        iter: impl IntoIterator<Item = K::Encode<'a>>,
    ) -> Result<(), StorageError> {
        self.0
            .extend(txn, iter.into_iter().map(|k| (k, [].as_slice())))
    }
}

#[cfg(test)]
mod tests {
    use dozer_types::borrow::IntoOwned;
    use tempdir::TempDir;

    use crate::lmdb_storage::{LmdbEnvironmentManager, LmdbEnvironmentOptions};

    use super::*;

    #[test]
    fn test_lmdb_set() {
        let temp_dir = TempDir::new("test_lmdb_set").unwrap();
        let mut env = LmdbEnvironmentManager::create_rw(
            temp_dir.path(),
            "test",
            LmdbEnvironmentOptions::default(),
        )
        .unwrap();
        let set = LmdbSet::<u32>::create(&mut env, Some("test")).unwrap();

        assert_eq!(set.count(env.txn_mut().unwrap()).unwrap(), 0);

        set.insert(env.txn_mut().unwrap(), &1).unwrap();

        assert_eq!(set.count(env.txn_mut().unwrap()).unwrap(), 1);

        assert!(set.contains(env.txn_mut().unwrap(), &1).unwrap());
        assert!(!set.contains(env.txn_mut().unwrap(), &2).unwrap());

        assert!(!set.remove(env.txn_mut().unwrap(), &2).unwrap());
        assert!(set.remove(env.txn_mut().unwrap(), &1).unwrap());
        assert_eq!(set.count(env.txn_mut().unwrap()).unwrap(), 0);

        set.extend(env.txn_mut().unwrap(), [&5, &4, &3]).unwrap();
        assert_eq!(
            set.iter(env.txn_mut().unwrap())
                .unwrap()
                .map(|result| result.map(IntoOwned::into_owned))
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![3, 4, 5]
        );

        set.clear(env.txn_mut().unwrap()).unwrap();
        assert_eq!(
            set.iter(env.txn_mut().unwrap())
                .unwrap()
                .map(|result| result.map(IntoOwned::into_owned))
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            Vec::<u32>::new()
        );
    }
}
