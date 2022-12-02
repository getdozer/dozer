use std::path::PathBuf;

pub mod batched_writer;
pub mod cache;
pub mod comparator;
pub mod indexer;
pub mod query;
pub mod utils;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, Default)]
pub struct CacheOptions {
    pub common: CacheCommonOptions,
    pub kind: CacheOptionsKind,
}

#[derive(Clone, Debug)]
pub enum CacheOptionsKind {
    // Write Options
    Write(CacheWriteOptions),

    // Read Options
    ReadOnly(CacheReadOptions),
}

impl Default for CacheOptionsKind {
    fn default() -> Self {
        Self::Write(CacheWriteOptions::default())
    }
}

#[derive(Clone, Debug)]
pub struct CacheCommonOptions {
    // Total number of readers allowed
    pub max_readers: u32,
    // Max no of dbs
    pub max_db_size: u32,

    /// The chunk size when calculating intersection of index queries.
    pub intersection_chunk_size: usize,

    // Provide a path where db will be created. If nothing is provided, will default to a temp location.
    pub path: Option<PathBuf>,
}

impl Default for CacheCommonOptions {
    fn default() -> Self {
        Self {
            max_readers: 10,
            max_db_size: 1000,
            intersection_chunk_size: 100,
            path: None,
        }
    }
}

impl CacheCommonOptions {
    pub fn set_path(&mut self, path: PathBuf) {
        self.path = Some(path);
    }
}

#[derive(Clone, Debug, Default)]
pub struct CacheReadOptions {}

#[derive(Clone, Debug)]
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
