use dozer_tracing::Labels;
use dozer_types::parking_lot::Mutex;
use std::collections::HashSet;
use std::path::PathBuf;
use std::{fmt::Debug, sync::Arc};

use dozer_types::types::{Record, SchemaWithIndex};

use super::{
    super::{RoCache, RwCache},
    indexing::IndexingThreadPool,
};
use crate::cache::expression::QueryExpression;
use crate::cache::{CacheRecord, CacheWriteOptions, CommitState, RecordMeta, UpsertResult};
use crate::errors::CacheError;

pub mod dump_restore;
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
    /// Db path will be `PathBuf.join(name)`.
    pub path: Option<(PathBuf, String)>,

    /// The labels to attach to the cache.
    pub labels: Labels,
}

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            max_readers: 1000,
            max_db_size: 1000,
            max_size: 1024 * 1024 * 1024,
            intersection_chunk_size: 100,
            path: None,
            labels: Labels::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LmdbRoCache {
    pub main_env: RoMainEnvironment,
    pub secondary_envs: Vec<RoSecondaryEnvironment>,
}

impl LmdbRoCache {
    pub fn new(options: CacheOptions) -> Result<Self, CacheError> {
        let main_env = RoMainEnvironment::new(options.clone())?;
        let secondary_envs = (0..main_env.schema().1.len())
            .map(|index| {
                RoSecondaryEnvironment::new(secondary_environment_name(index), options.clone())
            })
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
        connections: Option<&HashSet<String>>,
        options: CacheOptions,
        write_options: CacheWriteOptions,
        indexing_thread_pool: Arc<Mutex<IndexingThreadPool>>,
    ) -> Result<Self, CacheError> {
        let rw_main_env =
            RwMainEnvironment::new(schema, connections, options.clone(), write_options)?;

        let options = CacheOptions {
            path: Some((
                rw_main_env.base_path().to_path_buf(),
                rw_main_env.name().to_string(),
            )),
            ..options
        };
        let ro_main_env = rw_main_env.share();

        let mut rw_secondary_envs = vec![];
        let mut ro_secondary_envs = vec![];
        for (index, index_definition) in ro_main_env.schema().1.iter().enumerate() {
            let name = secondary_environment_name(index);
            let rw_secondary_env =
                RwSecondaryEnvironment::new(index_definition, name.clone(), options.clone())?;
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

    fn labels(&self) -> &Labels {
        self.main_env().labels()
    }

    fn get(&self, key: &[u8]) -> Result<CacheRecord, CacheError> {
        self.main_env().get(key)
    }

    fn count(&self, query: &QueryExpression) -> Result<usize, CacheError> {
        LmdbQueryHandler::new(self, query).count()
    }

    fn query(&self, query: &QueryExpression) -> Result<Vec<CacheRecord>, CacheError> {
        LmdbQueryHandler::new(self, query).query()
    }

    fn get_schema(&self) -> &SchemaWithIndex {
        self.main_env().schema()
    }

    fn get_commit_state(&self) -> Result<Option<CommitState>, CacheError> {
        self.main_env().commit_state()
    }

    fn is_snapshotting_done(&self) -> Result<bool, CacheError> {
        self.main_env().is_snapshotting_done()
    }
}

impl RwCache for LmdbRwCache {
    fn insert(&mut self, record: &Record) -> Result<UpsertResult, CacheError> {
        let span = dozer_types::tracing::span!(dozer_types::tracing::Level::TRACE, "insert_cache");
        let _enter = span.enter();
        self.main_env.insert(record)
    }

    fn delete(&mut self, record: &Record) -> Result<Option<RecordMeta>, CacheError> {
        self.main_env.delete(record)
    }

    fn update(&mut self, old: &Record, new: &Record) -> Result<UpsertResult, CacheError> {
        self.main_env.update(old, new)
    }

    fn set_connection_snapshotting_done(
        &mut self,
        connection_name: &str,
    ) -> Result<(), CacheError> {
        self.main_env
            .set_connection_snapshotting_done(connection_name)
    }

    fn commit(&mut self, state: &CommitState) -> Result<(), CacheError> {
        self.main_env.commit(state)?;
        self.indexing_thread_pool.lock().wake(self.labels());
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
