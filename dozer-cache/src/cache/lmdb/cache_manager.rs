use std::path::Path;
use std::{path::PathBuf, sync::Arc};

use dozer_storage::lmdb::RoTransaction;
use dozer_storage::{
    lmdb_storage::LmdbEnvironmentManager, LmdbEnvironment, LmdbMap, RoLmdbEnvironment,
    RwLmdbEnvironment,
};
use dozer_types::borrow::Cow;
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
    alias_to_real_name: LmdbMap<String, String>,
    env: RoLmdbEnvironment,
}

impl LmdbRoCacheManager {
    pub fn new(options: CacheManagerOptions) -> Result<Self, CacheError> {
        let base_path = options
            .path
            .as_deref()
            .ok_or(CacheError::PathNotInitialized)?;
        let env = LmdbEnvironmentManager::create_ro(
            base_path,
            LMDB_CACHE_MANAGER_ALIAS_ENV_NAME,
            Default::default(),
        )?;
        let base_path = base_path.to_path_buf();
        let alias_to_real_name = LmdbMap::open(&env, None)?;
        Ok(Self {
            options,
            base_path,
            alias_to_real_name,
            env,
        })
    }
}

impl RoCacheManager for LmdbRoCacheManager {
    fn open_ro_cache(&self, name: &str) -> Result<Option<Box<dyn RoCache>>, CacheError> {
        let txn = self.env.begin_txn()?;
        let real_name = resolve_alias(&txn, self.alias_to_real_name, name)?;
        open_ro_cache(&self.base_path, real_name, &self.options)
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
    fn open_ro_cache(&self, name: &str) -> Result<Option<Box<dyn RoCache>>, CacheError> {
        let env = self.env.read();
        let txn = env.begin_txn()?;
        let real_name = resolve_alias(&txn, self.alias_to_real_name, name)?;

        // Check if the cache is already opened.
        if let Some(cache) = self.indexing_thread_pool.lock().find_cache(real_name) {
            return Ok(Some(Box::new(cache)));
        }

        open_ro_cache(&self.base_path, real_name, &self.options)
    }
}

impl RwCacheManager for LmdbRwCacheManager {
    fn open_rw_cache(
        &self,
        name: &str,
        write_options: CacheWriteOptions,
    ) -> Result<Option<Box<dyn RwCache>>, CacheError> {
        let env = self.env.read();
        let txn = env.begin_txn()?;
        let real_name = resolve_alias(&txn, self.alias_to_real_name, name)?;

        let cache: Option<Box<dyn RwCache>> =
            if LmdbEnvironmentManager::exists(&self.base_path, real_name) {
                let cache = LmdbRwCache::new(
                    None,
                    &cache_options(&self.options, self.base_path.clone(), real_name.to_string()),
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
        schema: Schema,
        indexes: Vec<IndexDefinition>,
        write_options: CacheWriteOptions,
    ) -> Result<Box<dyn RwCache>, CacheError> {
        let name = self.generate_unique_name();
        let cache = LmdbRwCache::new(
            Some(&(schema, indexes)),
            &cache_options(&self.options, self.base_path.clone(), name),
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

impl LmdbRwCacheManager {
    fn generate_unique_name(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
}

fn resolve_alias<'a>(
    txn: &'a RoTransaction,
    alias_to_real_name: LmdbMap<String, String>,
    name: &'a str,
) -> Result<&'a str, CacheError> {
    Ok(match alias_to_real_name.get(txn, name)? {
        None => name,
        Some(real_name) => {
            let Cow::Borrowed(real_name) = real_name else {
                panic!("String should be zero-copy")
            };
            real_name
        }
    })
}

fn cache_options(options: &CacheManagerOptions, base_path: PathBuf, name: String) -> CacheOptions {
    CacheOptions {
        max_db_size: options.max_db_size,
        max_readers: options.max_readers,
        max_size: options.max_size,
        intersection_chunk_size: options.intersection_chunk_size,
        path: Some((base_path, name)),
    }
}

fn open_ro_cache(
    base_path: &Path,
    name: &str,
    options: &CacheManagerOptions,
) -> Result<Option<Box<dyn RoCache>>, CacheError> {
    let cache: Option<Box<dyn RoCache>> = if LmdbEnvironmentManager::exists(base_path, name) {
        let cache = LmdbRoCache::new(&cache_options(
            options,
            base_path.to_path_buf(),
            name.to_string(),
        ))?;
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
        let real_name = cache_manager
            .create_cache(Schema::empty(), vec![], Default::default())
            .unwrap()
            .name()
            .to_string();
        // Test open with real name.
        assert_eq!(
            cache_manager
                .open_rw_cache(&real_name, Default::default())
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
                .open_rw_cache(alias, Default::default())
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
                .open_rw_cache(&real_name, Default::default())
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
            .create_cache(Schema::empty(), vec![], Default::default())
            .unwrap()
            .name()
            .to_string();
        cache_manager.create_alias(&real_name, &real_name2).unwrap();
        assert_eq!(
            cache_manager
                .open_rw_cache(&real_name, Default::default())
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
