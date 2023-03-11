use std::path::PathBuf;
use std::{fmt::Debug, sync::Arc};

use dozer_types::types::{Record, SchemaWithIndex};

use super::{
    super::{RoCache, RwCache},
    indexing::IndexingThreadPool,
};
use crate::cache::expression::QueryExpression;
use crate::cache::RecordWithId;
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
            max_size: 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug)]
pub struct LmdbRwCache {
    main_env: RwMainEnvironment,
    secondary_envs: Vec<RoSecondaryEnvironment>,
}

impl LmdbRwCache {
    pub fn new(
        schema: Option<&SchemaWithIndex>,
        common_options: &CacheCommonOptions,
        write_options: CacheWriteOptions,
        indexing_thread_pool: &mut IndexingThreadPool,
    ) -> Result<Self, CacheError> {
        let rw_main_env = RwMainEnvironment::new(schema, common_options, write_options)?;

        let common_options = CacheCommonOptions {
            path: Some((
                rw_main_env.base_path().to_path_buf(),
                rw_main_env.name().to_string(),
            )),
            ..*common_options
        };
        let ro_main_env = Arc::new(RoMainEnvironment::new(&common_options)?);

        let mut ro_secondary_envs = vec![];
        for (index, index_definition) in ro_main_env.schema().1.iter().enumerate() {
            let name = secondary_environment_name(index);
            let rw_secondary_env = RwSecondaryEnvironment::new(
                Some(index_definition),
                name.clone(),
                &common_options,
                write_options,
            )?;
            indexing_thread_pool.add_indexing_task(ro_main_env.clone(), Arc::new(rw_secondary_env));

            let ro_secondary_env = RoSecondaryEnvironment::new(name, &common_options)?;
            ro_secondary_envs.push(ro_secondary_env);
        }

        Ok(Self {
            main_env: rw_main_env,
            secondary_envs: ro_secondary_envs,
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
        Ok(record_id)
    }

    fn delete(&self, key: &[u8]) -> Result<u32, CacheError> {
        let version = self.main_env.delete(key)?;
        Ok(version)
    }

    fn update(&self, key: &[u8], record: &mut Record) -> Result<u32, CacheError> {
        let version = self.delete(key)?;
        self.insert(record)?;
        Ok(version)
    }

    fn commit(&self) -> Result<(), CacheError> {
        self.main_env.commit()?;
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
