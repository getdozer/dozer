use dozer_types::borrow::IntoOwned;
use lmdb::{RwTransaction, Transaction};

use crate::{
    errors::StorageError,
    lmdb_storage::{LmdbEnvironment, RwLmdbEnvironment},
    LmdbOption,
};

#[derive(Debug, Clone, Copy)]
pub struct LmdbCounter(LmdbOption<u64>);

impl LmdbCounter {
    pub fn create(env: &mut RwLmdbEnvironment, name: Option<&str>) -> Result<Self, StorageError> {
        LmdbOption::create(env, name).map(Self)
    }

    pub fn open<E: LmdbEnvironment>(env: &E, name: Option<&str>) -> Result<Self, StorageError> {
        LmdbOption::open(env, name).map(Self)
    }

    pub fn load(&self, txn: &impl Transaction) -> Result<u64, StorageError> {
        self.0
            .load(txn)
            .map(|value| value.map_or(0, IntoOwned::into_owned))
    }

    pub fn store(&self, txn: &mut RwTransaction, value: u64) -> Result<(), StorageError> {
        self.0.store(txn, &value)
    }

    pub fn fetch_add(&self, txn: &mut RwTransaction, value: u64) -> Result<u64, StorageError> {
        let current = self.load(txn)?;
        self.store(txn, current + value)?;
        Ok(current)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::{
        lmdb_storage::{LmdbEnvironmentManager, LmdbEnvironmentOptions},
        LmdbCounter,
    };

    #[test]
    fn test_lmdb_counter() {
        let temp_dir = TempDir::new("test_lmdb_counter").unwrap();
        let mut env = LmdbEnvironmentManager::create_rw(
            temp_dir.path(),
            "test",
            LmdbEnvironmentOptions::default(),
        )
        .unwrap();
        let counter = LmdbCounter::create(&mut env, None).unwrap();

        assert_eq!(counter.load(env.txn_mut().unwrap()).unwrap(), 0);

        counter.store(env.txn_mut().unwrap(), 0).unwrap();
        assert_eq!(counter.load(env.txn_mut().unwrap()).unwrap(), 0);

        counter.store(env.txn_mut().unwrap(), 1).unwrap();
        assert_eq!(counter.load(env.txn_mut().unwrap()).unwrap(), 1);

        assert_eq!(counter.fetch_add(env.txn_mut().unwrap(), 1).unwrap(), 1);
        assert_eq!(counter.load(env.txn_mut().unwrap()).unwrap(), 2);
    }
}
