use std::{path::PathBuf, sync::Arc};

use dozer_storage::{lmdb_storage::LmdbEnvironmentManager, LmdbMap, RwLmdbEnvironment};
use dozer_types::labels::Labels;
use dozer_types::parking_lot::RwLock;
use dozer_types::{
    parking_lot::Mutex,
    types::{IndexDefinition, Schema},
};
use tempdir::TempDir;

use crate::cache::CacheWriteOptions;
use crate::{
    cache::{RoCache, RoCacheManager, RwCache, RwCacheManager},
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
pub struct LmdbRoCacheManager {
    options: CacheManagerOptions,
    base_path: PathBuf,
}

impl LmdbRoCacheManager {
    pub fn new(options: CacheManagerOptions) -> Result<Self, CacheError> {
        let base_path = options
            .path
            .as_deref()
            .ok_or(CacheError::PathNotInitialized)?;
        let base_path = base_path.to_path_buf();
        Ok(Self { options, base_path })
    }
}

impl RoCacheManager for LmdbRoCacheManager {
    fn open_ro_cache(&self, labels: Labels) -> Result<Option<Box<dyn RoCache>>, CacheError> {
        open_ro_cache(self.base_path.clone(), labels, &self.options)
    }
}

#[derive(Debug)]
pub struct LmdbRwCacheManager {
    options: CacheManagerOptions,
    base_path: PathBuf,
    alias_to_real_name: LmdbMap<String, String>,
    env: RwLock<RwLmdbEnvironment>,
    indexing_thread_pool: Arc<Mutex<IndexingThreadPool>>,
    _temp_dir: Option<TempDir>,
}

impl LmdbRwCacheManager {
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
        let alias_to_real_name = LmdbMap::create(&mut env, None)?;

        let indexing_thread_pool = Arc::new(Mutex::new(IndexingThreadPool::new(
            options.num_indexing_threads,
        )));

        Ok(Self {
            options,
            base_path,
            alias_to_real_name,
            env: RwLock::new(env),
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

impl RoCacheManager for LmdbRwCacheManager {
    fn open_ro_cache(&self, labels: Labels) -> Result<Option<Box<dyn RoCache>>, CacheError> {
        // Check if the cache is already opened.
        if let Some(cache) = self.indexing_thread_pool.lock().find_cache(&labels) {
            return Ok(Some(Box::new(cache)));
        }

        open_ro_cache(self.base_path.clone(), labels, &self.options)
    }
}

impl RwCacheManager for LmdbRwCacheManager {
    fn open_rw_cache(
        &self,
        labels: Labels,
        write_options: CacheWriteOptions,
    ) -> Result<Option<Box<dyn RwCache>>, CacheError> {
        let cache: Option<Box<dyn RwCache>> =
            if LmdbEnvironmentManager::exists(&self.base_path, &labels.to_non_empty_string()) {
                let cache = LmdbRwCache::new(
                    None,
                    &cache_options(&self.options, self.base_path.clone(), labels),
                    write_options,
                    self.indexing_thread_pool.clone(),
                )?;
                Some(Box::new(cache))
            } else {
                None
            };
        Ok(cache)
    }

    fn create_cache(
        &self,
        labels: Labels,
        schema: Schema,
        indexes: Vec<IndexDefinition>,
        write_options: CacheWriteOptions,
    ) -> Result<Box<dyn RwCache>, CacheError> {
        let cache = LmdbRwCache::new(
            Some(&(schema, indexes)),
            &cache_options(&self.options, self.base_path.clone(), labels),
            write_options,
            self.indexing_thread_pool.clone(),
        )?;
        Ok(Box::new(cache))
    }

    fn create_alias(&self, name: &str, alias: &str) -> Result<(), CacheError> {
        let mut env = self.env.write();
        self.alias_to_real_name
            .insert_overwrite(env.txn_mut()?, alias, name)?;
        env.commit()?;
        Ok(())
    }
}

const LMDB_CACHE_MANAGER_ALIAS_ENV_NAME: &str = "__DOZER_CACHE_MANAGER_ALIAS__";

fn cache_options(
    options: &CacheManagerOptions,
    base_path: PathBuf,
    labels: Labels,
) -> CacheOptions {
    CacheOptions {
        max_db_size: options.max_db_size,
        max_readers: options.max_readers,
        max_size: options.max_size,
        intersection_chunk_size: options.intersection_chunk_size,
        path: Some((base_path, labels)),
    }
}

fn open_ro_cache(
    base_path: PathBuf,
    labels: Labels,
    options: &CacheManagerOptions,
) -> Result<Option<Box<dyn RoCache>>, CacheError> {
    let cache: Option<Box<dyn RoCache>> =
        if LmdbEnvironmentManager::exists(&base_path, &labels.to_non_empty_string()) {
            let cache = LmdbRoCache::new(&cache_options(options, base_path, labels))?;
            Some(Box::new(cache))
        } else {
            None
        };
    Ok(cache)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lmdb_cache_manager() {
        let cache_manager = LmdbRwCacheManager::new(Default::default()).unwrap();
        let labels = cache_manager
            .create_cache(
                Default::default(),
                Schema::empty(),
                vec![],
                Default::default(),
            )
            .unwrap()
            .labels()
            .clone();
        // Test open with labels.
        assert_eq!(
            cache_manager
                .open_rw_cache(labels.clone(), Default::default())
                .unwrap()
                .unwrap()
                .labels(),
            &labels
        );
        assert_eq!(
            cache_manager
                .open_ro_cache(labels.clone())
                .unwrap()
                .unwrap()
                .labels(),
            &labels
        );
    }
}
