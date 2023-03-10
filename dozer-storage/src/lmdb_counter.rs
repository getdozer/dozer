use dozer_types::borrow::IntoOwned;
use lmdb::{RwTransaction, Transaction};

use crate::{errors::StorageError, lmdb_storage::CreateDatabase, LmdbOption};

#[derive(Debug, Clone, Copy)]
pub struct LmdbCounter(LmdbOption<u64>);

impl LmdbCounter {
    pub fn new<C: CreateDatabase>(
        c: &mut C,
        name: Option<&str>,
        create_if_not_exist: bool,
    ) -> Result<Self, StorageError> {
        LmdbOption::new(c, name, create_if_not_exist).map(Self)
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
        let mut env = LmdbEnvironmentManager::create(
            temp_dir.path(),
            "test",
            LmdbEnvironmentOptions::default(),
        )
        .unwrap();
        let counter = LmdbCounter::new(&mut env, None, true).unwrap();

        let txn = env.create_txn().unwrap();
        let mut txn = txn.write();

        assert_eq!(counter.load(txn.txn()).unwrap(), 0);

        counter.store(txn.txn_mut(), 0).unwrap();
        assert_eq!(counter.load(txn.txn()).unwrap(), 0);

        counter.store(txn.txn_mut(), 1).unwrap();
        assert_eq!(counter.load(txn.txn()).unwrap(), 1);

        assert_eq!(counter.fetch_add(txn.txn_mut(), 1).unwrap(), 1);
        assert_eq!(counter.load(txn.txn()).unwrap(), 2);
    }
}
