use std::fmt::Debug;
use std::path::PathBuf;

use dozer_storage::BeginTransaction;

use dozer_types::types::{Record, SchemaWithIndex};

use super::super::{RoCache, RwCache};
use crate::cache::expression::QueryExpression;
use crate::cache::RecordWithId;
use crate::errors::CacheError;

mod helper;
mod main_environment;
mod query;
mod secondary_environment;

use main_environment::{MainEnvironment, RoMainEnvironment, RwMainEnvironment};
use query::LmdbQueryHandler;
pub use secondary_environment::SecondaryEnvironment;
use secondary_environment::{RoSecondaryEnvironment, RwSecondaryEnvironment};

#[derive(Clone, Debug)]
pub struct CacheCommonOptions {
    // Total number of readers allowed
    pub max_readers: u32,
    // Max no of dbs
    pub max_db_size: u32,

    /// The chunk size when calculating intersection of index queries.
    pub intersection_chunk_size: usize,

    /// Provide a path where db will be created. If nothing is provided, will default to a temp location.
    /// Db path will be `PathBuf.join(String)`.
    pub path: Option<(PathBuf, String)>,
}

impl Default for CacheCommonOptions {
    fn default() -> Self {
        Self {
            max_readers: 1000,
            max_db_size: 1000,
            intersection_chunk_size: 100,
            path: None,
        }
    }
}

#[derive(Debug)]
pub struct LmdbRoCache {
    main_env: RoMainEnvironment,
    secondary_envs: Vec<RoSecondaryEnvironment>,
}

impl LmdbRoCache {
    pub fn new(options: &CacheCommonOptions) -> Result<Self, CacheError> {
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

#[derive(Debug, Clone, Copy)]
pub struct CacheWriteOptions {
    // Total size allocated for data in a memory mapped file.
    // This size is allocated at initialization.
    pub max_size: usize,
}

impl Default for CacheWriteOptions {
    fn default() -> Self {
        Self {
            max_size: 1024 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug)]
pub struct LmdbRwCache {
    main_env: RwMainEnvironment,
    secondary_envs: Vec<RwSecondaryEnvironment>,
}

impl LmdbRwCache {
    pub fn open(
        common_options: &CacheCommonOptions,
        write_options: CacheWriteOptions,
    ) -> Result<Self, CacheError> {
        let main_env = RwMainEnvironment::open(common_options, write_options)?;
        let secondary_envs = (0..main_env.schema().1.len())
            .map(|index| {
                RwSecondaryEnvironment::open(
                    secondary_environment_name(index),
                    common_options,
                    write_options,
                )
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            main_env,
            secondary_envs,
        })
    }

    pub fn create(
        schema: &SchemaWithIndex,
        common_options: &CacheCommonOptions,
        write_options: CacheWriteOptions,
    ) -> Result<Self, CacheError> {
        let main_env = RwMainEnvironment::create(schema, common_options, write_options)?;
        let secondary_envs = main_env
            .schema()
            .1
            .iter()
            .enumerate()
            .map(|(index, index_definition)| {
                RwSecondaryEnvironment::create(
                    index_definition,
                    secondary_environment_name(index),
                    common_options,
                    write_options,
                )
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            main_env,
            secondary_envs,
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
    fn insert(&self, record: &mut Record) -> Result<u64, CacheError> {
        let span = dozer_types::tracing::span!(dozer_types::tracing::Level::TRACE, "insert_cache");
        let _enter = span.enter();
        let record_id = self.main_env.insert(record)?;

        let span = dozer_types::tracing::span!(
            dozer_types::tracing::Level::TRACE,
            "build_indexes",
            record_id = record_id,
        );
        let _enter = span.enter();
        self.index()?;

        Ok(record_id)
    }

    fn delete(&self, key: &[u8]) -> Result<u32, CacheError> {
        let version = self.main_env.delete(key)?;
        self.index()?;
        Ok(version)
    }

    fn update(&self, key: &[u8], record: &mut Record) -> Result<u32, CacheError> {
        let version = self.delete(key)?;
        self.insert(record)?;
        self.index()?;
        Ok(version)
    }

    fn commit(&self) -> Result<(), CacheError> {
        self.main_env.commit()?;
        for secondary_env in &self.secondary_envs {
            secondary_env.commit()?;
        }
        Ok(())
    }
}

impl LmdbRwCache {
    fn index(&self) -> Result<(), CacheError> {
        let main_txn = self.main_env.begin_txn()?;
        for secondary_env in &self.secondary_envs {
            secondary_env.index(&main_txn, self.main_env.operation_log())?;
        }
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

    type SecondaryEnvironment = RwSecondaryEnvironment;
    fn secondary_env(&self, index: usize) -> &Self::SecondaryEnvironment {
        &self.secondary_envs[index]
    }
}

fn secondary_environment_name(index: usize) -> String {
    format!("{index}")
}
