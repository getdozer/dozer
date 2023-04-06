use dozer_types::models::api_endpoint::ConflictResolution;
use dozer_types::parking_lot::Mutex;
use std::path::PathBuf;
use std::{fmt::Debug, sync::Arc};

use dozer_types::types::{Record, SchemaWithIndex};

use super::{
    super::{RoCache, RwCache},
    indexing::IndexingThreadPool,
};
use crate::cache::expression::QueryExpression;
use crate::cache::{RecordMeta, RecordWithId, UpsertResult};
use crate::errors::CacheError;

mod main_environment;
mod query;
mod secondary_environment;

pub use main_environment::{MainEnvironment, RoMainEnvironment, RwMainEnvironment};
use query::LmdbQueryHandler;
pub use secondary_environment::{
    RoSecondaryEnvironment, RwSecondaryEnvironment, SecondaryEnvironment,
};

#[derive(Clone, Debug)]
pub struct CacheOptions {
    // Total number of readers allowed
    pub max_readers: u32,
    // Max no of dbs
    pub max_db_size: u32,
    // Total size allocated for data in a memory mapped file.
    // This size is allocated at initialization.
    pub max_size: usize,

    /// The chunk size when calculating intersection of index queries.
    pub intersection_chunk_size: usize,

    /// Provide a path where db will be created. If nothing is provided, will default to a temp location.
    /// Db path will be `PathBuf.join(String)`.
    pub path: Option<(PathBuf, String)>,
}

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            max_readers: 1000,
            max_db_size: 1000,
            max_size: 1024 * 1024 * 1024,
            intersection_chunk_size: 100,
            path: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LmdbRoCache {
    pub(crate) main_env: RoMainEnvironment,
    pub(crate) secondary_envs: Vec<RoSecondaryEnvironment>,
}

impl LmdbRoCache {
    pub fn new(options: &CacheOptions) -> Result<Self, CacheError> {
        let main_env = RoMainEnvironment::new(options)?;
        let secondary_envs = (0..main_env.schema().1.len())
            .map(|index| RoSecondaryEnvironment::new(secondary_environment_name(index), options))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            main_env,
            secondary_envs,
        })
    }
}

#[derive(Debug)]
pub struct LmdbRwCache {
    main_env: RwMainEnvironment,
    secondary_envs: Vec<RoSecondaryEnvironment>,
    indexing_thread_pool: Arc<Mutex<IndexingThreadPool>>,
}

impl LmdbRwCache {
    pub fn new(
        schema: Option<&SchemaWithIndex>,
        options: &CacheOptions,
        indexing_thread_pool: Arc<Mutex<IndexingThreadPool>>,
        conflict_resolution: ConflictResolution,
    ) -> Result<Self, CacheError> {
        let rw_main_env = RwMainEnvironment::new(schema, options, conflict_resolution)?;

        let options = CacheOptions {
            path: Some((
                rw_main_env.base_path().to_path_buf(),
                rw_main_env.name().to_string(),
            )),
            ..*options
        };
        let ro_main_env = rw_main_env.share();

        let mut rw_secondary_envs = vec![];
        let mut ro_secondary_envs = vec![];
        for (index, index_definition) in ro_main_env.schema().1.iter().enumerate() {
            let name = secondary_environment_name(index);
            let rw_secondary_env =
                RwSecondaryEnvironment::new(index_definition, name.clone(), &options)?;
            let ro_secondary_env = rw_secondary_env.share();

            rw_secondary_envs.push(rw_secondary_env);
            ro_secondary_envs.push(ro_secondary_env);
        }

        indexing_thread_pool
            .lock()
            .add_cache(ro_main_env, rw_secondary_envs);

        Ok(Self {
            main_env: rw_main_env,
            secondary_envs: ro_secondary_envs,
            indexing_thread_pool,
        })
    }
}

impl<C: LmdbCache> RoCache for C {
    fn name(&self) -> &str {
        self.main_env().name()
    }

    fn get(&self, key: &[u8]) -> Result<RecordWithId, CacheError> {
        self.main_env().get(key)
    }

    fn count(&self, query: &QueryExpression) -> Result<usize, CacheError> {
        LmdbQueryHandler::new(self, query).count()
    }

    fn query(&self, query: &QueryExpression) -> Result<Vec<RecordWithId>, CacheError> {
        LmdbQueryHandler::new(self, query).query()
    }

    fn get_schema(&self) -> &SchemaWithIndex {
        self.main_env().schema()
    }
}

impl RwCache for LmdbRwCache {
    fn insert(&mut self, record: &Record) -> Result<UpsertResult, CacheError> {
        let span = dozer_types::tracing::span!(dozer_types::tracing::Level::TRACE, "insert_cache");
        let _enter = span.enter();
        self.main_env.insert(record)
    }

    fn delete(&mut self, key: &[u8]) -> Result<Option<RecordMeta>, CacheError> {
        self.main_env.delete(key)
    }

    fn update(&mut self, key: &[u8], record: &Record) -> Result<UpsertResult, CacheError> {
        self.main_env.update(key, record)
    }

    fn commit(&mut self) -> Result<(), CacheError> {
        self.main_env.commit()?;
        self.indexing_thread_pool.lock().wake(self.name());
        Ok(())
    }
}

pub trait LmdbCache: Send + Sync + Debug {
    type MainEnvironment: MainEnvironment;

    fn main_env(&self) -> &Self::MainEnvironment;

    type SecondaryEnvironment: SecondaryEnvironment;

    fn secondary_env(&self, index: usize) -> &Self::SecondaryEnvironment;
}

impl LmdbCache for LmdbRoCache {
    type MainEnvironment = RoMainEnvironment;
    fn main_env(&self) -> &Self::MainEnvironment {
        &self.main_env
    }

    type SecondaryEnvironment = RoSecondaryEnvironment;
    fn secondary_env(&self, index: usize) -> &Self::SecondaryEnvironment {
        &self.secondary_envs[index]
    }
}

impl LmdbCache for LmdbRwCache {
    type MainEnvironment = RwMainEnvironment;
    fn main_env(&self) -> &Self::MainEnvironment {
        &self.main_env
    }

    type SecondaryEnvironment = RoSecondaryEnvironment;
    fn secondary_env(&self, index: usize) -> &Self::SecondaryEnvironment {
        &self.secondary_envs[index]
    }
}

fn secondary_environment_name(index: usize) -> String {
    format!("{index}")
}
