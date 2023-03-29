use std::path::PathBuf;

use dozer_storage::{
    errors::StorageError,
    lmdb::{Database, DatabaseFlags},
    lmdb_storage::LmdbEnvironmentManager,
    RwLmdbEnvironment,
};
use dozer_types::models::api_endpoint::ConflictResolution;
use dozer_types::{
    parking_lot::Mutex,
    types::{IndexDefinition, Schema},
};
use tempdir::TempDir;

use crate::{
    cache::{CacheManager, RoCache, RwCache},
    errors::CacheError,
};

use super::{
    cache::{CacheOptions, LmdbRoCache, LmdbRwCache},
    indexing::IndexingThreadPool,
};

#[derive(Debug, Clone)]
pub struct CacheManagerOptions {
    // Total number of readers allowed
    pub max_readers: u32,
    // Max no of dbs
    pub max_db_size: u32,

    // Total size allocated for data in a memory mapped file.
    // This size is allocated at initialization.
    pub max_size: usize,

    /// The chunk size when calculating intersection of index queries.
    pub intersection_chunk_size: usize,

    /// Provide a path where db will be created. If nothing is provided, will default to a temp directory.
    pub path: Option<PathBuf>,

    /// Number of threads in the indexing thread pool.
    pub num_indexing_threads: usize,
}

impl Default for CacheManagerOptions {
    fn default() -> Self {
        let cache_options = CacheOptions::default();
        Self {
            max_readers: cache_options.max_readers,
            max_db_size: cache_options.max_db_size,
            intersection_chunk_size: cache_options.intersection_chunk_size,
            max_size: cache_options.max_size,
            path: None,
            num_indexing_threads: 4,
        }
    }
}

#[derive(Debug)]
pub struct LmdbCacheManager {
    options: CacheManagerOptions,
    base_path: PathBuf,
    alias_db: Database,
    env: Mutex<RwLmdbEnvironment>,
    indexing_thread_pool: Mutex<IndexingThreadPool>,
    _temp_dir: Option<TempDir>,
}

impl LmdbCacheManager {
    pub fn new(options: CacheManagerOptions) -> Result<Self, CacheError> {
        let (temp_dir, base_path) = match &options.path {
            Some(path) => {
                std::fs::create_dir_all(path).map_err(|e| CacheError::Io(path.clone(), e))?;
                (None, path.clone())
            }
            None => {
                let temp_dir = TempDir::new("dozer").expect("Unable to create temp dir");
                let base_path = temp_dir.path().to_path_buf();
                (Some(temp_dir), base_path)
            }
        };

        let mut env = LmdbEnvironmentManager::create_rw(
            &base_path,
            LMDB_CACHE_MANAGER_ALIAS_ENV_NAME,
            Default::default(),
        )?;
        let alias_db = env.create_database(None, DatabaseFlags::empty())?;

        let indexing_thread_pool =
            Mutex::new(IndexingThreadPool::new(options.num_indexing_threads));

        Ok(Self {
            options,
            base_path,
            alias_db,
            env: Mutex::new(env),
            indexing_thread_pool,
            _temp_dir: temp_dir,
        })
    }

    /// Blocks current thread until all secondary indexes are up to date with the last cache commit.
    ///
    /// If any cache commits during this call in another thread, those commits may or may not be indexed when this function returns.
    pub fn wait_until_indexing_catchup(&self) {
        self.indexing_thread_pool.lock().wait_until_catchup();
    }
}

impl CacheManager for LmdbCacheManager {
    fn open_rw_cache(
        &self,
        name: &str,
        conflict_resolution: ConflictResolution,
    ) -> Result<Option<Box<dyn RwCache>>, CacheError> {
        let mut env = self.env.lock();
        // Open a new transaction to make sure we get the latest changes.
        env.commit()?;
        let real_name = self.resolve_alias(name, &mut env)?;
        let real_name = real_name.as_deref().unwrap_or(name);
        let cache: Option<Box<dyn RwCache>> =
            if LmdbEnvironmentManager::exists(&self.base_path, real_name) {
                let cache = LmdbRwCache::new(
                    None,
                    &self.cache_options(real_name.to_string()),
                    &mut self.indexing_thread_pool.lock(),
                    conflict_resolution,
                )?;
                Some(Box::new(cache))
            } else {
                None
            };
        Ok(cache)
    }

    fn open_ro_cache(&self, name: &str) -> Result<Option<Box<dyn RoCache>>, CacheError> {
        let mut env = self.env.lock();
        // Open a new transaction to make sure we get the latest changes.
        env.commit()?;
        let real_name = self.resolve_alias(name, &mut env)?;
        let real_name = real_name.as_deref().unwrap_or(name);

        // Check if the cache is already opened.
        if let Some(cache) = self.indexing_thread_pool.lock().find_cache(real_name) {
            return Ok(Some(Box::new(cache)));
        }

        let cache: Option<Box<dyn RoCache>> =
            if LmdbEnvironmentManager::exists(&self.base_path, real_name) {
                let cache = LmdbRoCache::new(&self.cache_options(real_name.to_string()))?;
                Some(Box::new(cache))
            } else {
                None
            };
        Ok(cache)
    }

    fn create_cache(
        &self,
        schema: Schema,
        indexes: Vec<IndexDefinition>,
        conflict_resolution: ConflictResolution,
    ) -> Result<Box<dyn RwCache>, CacheError> {
        let name = self.generate_unique_name();
        let cache = LmdbRwCache::new(
            Some(&(schema, indexes)),
            &self.cache_options(name),
            &mut self.indexing_thread_pool.lock(),
            conflict_resolution,
        )?;
        Ok(Box::new(cache))
    }

    fn create_alias(&self, name: &str, alias: &str) -> Result<(), CacheError> {
        let mut env = self.env.lock();
        env.put(self.alias_db, alias.as_bytes(), name.as_bytes())?;
        env.commit()?;
        Ok(())
    }
}

const LMDB_CACHE_MANAGER_ALIAS_ENV_NAME: &str = "__DOZER_CACHE_MANAGER_ALIAS__";

impl LmdbCacheManager {
    fn cache_options(&self, name: String) -> CacheOptions {
        CacheOptions {
            max_db_size: self.options.max_db_size,
            max_readers: self.options.max_readers,
            max_size: self.options.max_size,
            intersection_chunk_size: self.options.intersection_chunk_size,
            path: Some((self.base_path.clone(), name)),
        }
    }

    fn generate_unique_name(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    fn resolve_alias(
        &self,
        alias: &str,
        txn: &mut RwLmdbEnvironment,
    ) -> Result<Option<String>, StorageError> {
        let result = txn
            .get(self.alias_db, alias.as_bytes())
            .map(|bytes| {
                bytes.map(|bytes| {
                    std::str::from_utf8(bytes).expect("Real names should always be utf8 string")
                })
            })?
            .map(|name| name.to_string());
        txn.commit()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lmdb_cache_manager() {
        let cache_manager = LmdbCacheManager::new(Default::default()).unwrap();
        let real_name = cache_manager
            .create_cache(Schema::empty(), vec![], ConflictResolution::default())
            .unwrap()
            .name()
            .to_string();
        // Test open with real name.
        assert_eq!(
            cache_manager
                .open_rw_cache(&real_name, ConflictResolution::default())
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
            cache_manager
                .open_rw_cache(alias, ConflictResolution::default())
                .unwrap()
                .unwrap()
                .name(),
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
                .open_rw_cache(&real_name, ConflictResolution::default())
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
            .create_cache(Schema::empty(), vec![], ConflictResolution::default())
            .unwrap()
            .name()
            .to_string();
        cache_manager.create_alias(&real_name, &real_name2).unwrap();
        assert_eq!(
            cache_manager
                .open_rw_cache(&real_name, ConflictResolution::default())
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
