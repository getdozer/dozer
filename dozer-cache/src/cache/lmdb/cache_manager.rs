use std::path::PathBuf;

use dozer_storage::{
    errors::StorageError,
    lmdb::{Database, DatabaseFlags},
    lmdb_storage::{LmdbEnvironmentManager, LmdbExclusiveTransaction, SharedTransaction},
};
use dozer_types::types::{IndexDefinition, Schema};
use tempdir::TempDir;

use crate::{
    cache::{CacheManager, RoCache, RwCache},
    errors::CacheError,
};

use super::cache::{CacheCommonOptions, CacheWriteOptions, LmdbRoCache, LmdbRwCache};

#[derive(Debug, Clone)]
pub struct CacheManagerOptions {
    // Total number of readers allowed
    pub max_readers: u32,
    // Max no of dbs
    pub max_db_size: u32,

    /// The chunk size when calculating intersection of index queries.
    pub intersection_chunk_size: usize,

    // Total size allocated for data in a memory mapped file.
    // This size is allocated at initialization.
    pub max_size: usize,

    /// Provide a path where db will be created. If nothing is provided, will default to a temp directory.
    pub path: Option<PathBuf>,
}

impl Default for CacheManagerOptions {
    fn default() -> Self {
        let cache_common_options = CacheCommonOptions::default();
        let cache_write_options = CacheWriteOptions::default();
        Self {
            max_readers: cache_common_options.max_readers,
            max_db_size: cache_common_options.max_db_size,
            intersection_chunk_size: cache_common_options.intersection_chunk_size,
            max_size: cache_write_options.max_size,
            path: None,
        }
    }
}

#[derive(Debug)]
pub struct LmdbCacheManager {
    options: CacheManagerOptions,
    base_path: PathBuf,
    alias_db: Database,
    txn: SharedTransaction,
    _temp_dir: Option<TempDir>,
}

impl LmdbCacheManager {
    pub fn new(options: CacheManagerOptions) -> Result<Self, CacheError> {
        let (temp_dir, base_path) = match &options.path {
            Some(path) => {
                std::fs::create_dir_all(path).map_err(|e| CacheError::Internal(Box::new(e)))?;
                (None, path.clone())
            }
            None => {
                let temp_dir = TempDir::new("dozer").expect("Unable to create temp dir");
                let base_path = temp_dir.path().to_path_buf();
                (Some(temp_dir), base_path)
            }
        };

        let mut env = LmdbEnvironmentManager::create(
            &base_path,
            LMDB_CACHE_MANAGER_ALIAS_ENV_NAME,
            Default::default(),
        )?;
        let alias_db = env.create_database(None, Some(DatabaseFlags::empty()))?;
        let txn = env.create_txn()?;

        Ok(Self {
            options,
            base_path,
            alias_db,
            txn,
            _temp_dir: temp_dir,
        })
    }
}

impl CacheManager for LmdbCacheManager {
    fn open_rw_cache(&self, name: &str) -> Result<Option<Box<dyn RwCache>>, CacheError> {
        let mut txn = self.txn.write();
        // Open a new transaction to make sure we get the latest changes.
        txn.commit_and_renew()?;
        let real_name = self.resolve_alias(name, &txn)?.unwrap_or(name);
        let cache: Option<Box<dyn RwCache>> =
            if LmdbEnvironmentManager::exists(&self.base_path, real_name) {
                let cache = LmdbRwCache::open(
                    self.cache_common_options(real_name.to_string()),
                    self.cache_write_options(),
                )?;
                Some(Box::new(cache))
            } else {
                None
            };
        Ok(cache)
    }

    fn open_ro_cache(&self, name: &str) -> Result<Option<Box<dyn RoCache>>, CacheError> {
        let mut txn = self.txn.write();
        // Open a new transaction to make sure we get the latest changes.
        txn.commit_and_renew()?;
        let real_name = self.resolve_alias(name, &txn)?.unwrap_or(name);
        let cache: Option<Box<dyn RoCache>> =
            if LmdbEnvironmentManager::exists(&self.base_path, real_name) {
                let cache = LmdbRoCache::new(self.cache_common_options(real_name.to_string()))?;
                Some(Box::new(cache))
            } else {
                None
            };
        Ok(cache)
    }

    fn create_cache(
        &self,
        schemas: Vec<(String, Schema, Vec<IndexDefinition>)>,
    ) -> Result<Box<dyn RwCache>, CacheError> {
        let name = self.generate_unique_name();
        let cache = LmdbRwCache::create(
            schemas,
            self.cache_common_options(name),
            self.cache_write_options(),
        )?;
        Ok(Box::new(cache))
    }

    fn create_alias(&self, name: &str, alias: &str) -> Result<(), CacheError> {
        let mut txn = self.txn.write();
        txn.put(self.alias_db, alias.as_bytes(), name.as_bytes())?;
        txn.commit_and_renew()?;
        Ok(())
    }
}

const LMDB_CACHE_MANAGER_ALIAS_ENV_NAME: &str = "__DOZER_CACHE_MANAGER_ALIAS__";

impl LmdbCacheManager {
    fn cache_common_options(&self, name: String) -> CacheCommonOptions {
        CacheCommonOptions {
            max_db_size: self.options.max_db_size,
            max_readers: self.options.max_readers,
            intersection_chunk_size: self.options.intersection_chunk_size,
            path: Some((self.base_path.clone(), name)),
        }
    }

    fn cache_write_options(&self) -> CacheWriteOptions {
        CacheWriteOptions {
            max_size: self.options.max_size,
        }
    }

    fn generate_unique_name(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    fn resolve_alias<'a>(
        &self,
        alias: &str,
        txn: &'a LmdbExclusiveTransaction,
    ) -> Result<Option<&'a str>, StorageError> {
        txn.get(self.alias_db, alias.as_bytes()).map(|bytes| {
            bytes.map(|bytes| {
                std::str::from_utf8(bytes).expect("Real names should always be utf8 string")
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lmdb_cache_manager() {
        let cache_manager = LmdbCacheManager::new(Default::default()).unwrap();
        let real_name = cache_manager
            .create_cache(vec![])
            .unwrap()
            .name()
            .to_string();
        // Test open with real name.
        assert_eq!(
            cache_manager
                .open_rw_cache(&real_name)
                .unwrap()
                .unwrap()
                .name(),
            real_name
        );
        assert_eq!(
            cache_manager
                .open_ro_cache(&real_name)
                .unwrap()
                .unwrap()
                .name(),
            real_name
        );
        // Test open with alias.
        let alias = "alias";
        cache_manager.create_alias(&real_name, alias).unwrap();
        assert_eq!(
            cache_manager.open_rw_cache(alias).unwrap().unwrap().name(),
            real_name
        );
        assert_eq!(
            cache_manager.open_ro_cache(alias).unwrap().unwrap().name(),
            real_name
        );
        // Test duplicate alias and real name.
        cache_manager.create_alias(&real_name, &real_name).unwrap();
        assert_eq!(
            cache_manager
                .open_rw_cache(&real_name)
                .unwrap()
                .unwrap()
                .name(),
            real_name
        );
        assert_eq!(
            cache_manager
                .open_ro_cache(&real_name)
                .unwrap()
                .unwrap()
                .name(),
            real_name
        );
        // If name is both alias and real name, alias shadows real name.
        let real_name2 = cache_manager
            .create_cache(vec![])
            .unwrap()
            .name()
            .to_string();
        cache_manager.create_alias(&real_name, &real_name2).unwrap();
        assert_eq!(
            cache_manager
                .open_rw_cache(&real_name)
                .unwrap()
                .unwrap()
                .name(),
            real_name
        );
        assert_eq!(
            cache_manager
                .open_ro_cache(&real_name)
                .unwrap()
                .unwrap()
                .name(),
            real_name
        );
    }
}
