use std::path::PathBuf;

pub mod cache;
pub mod indexer;
pub mod query;
pub mod utils;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub enum CacheOptions {
    // Write Options
    Write(CacheWriteOptions),

    // Read Options
    ReadOnly(CacheReadOptions),
}

impl Default for CacheOptions {
    fn default() -> Self {
        CacheOptions::Write(CacheWriteOptions::default())
    }
}
#[derive(Clone, Debug)]
pub struct CacheReadOptions {
    // Total number of readers allowed
    pub max_readers: u32,

    // Max no of dbs
    pub max_db_size: u32,

    // Absolute path
    pub path: Option<PathBuf>,
}
impl Default for CacheReadOptions {
    fn default() -> Self {
        Self {
            max_readers: 10,
            // Max no of dbs
            max_db_size: 1000,
            path: None,
        }
    }
}
impl CacheReadOptions {
    pub fn set_path(&mut self, path: PathBuf) {
        self.path = Some(path);
    }
}

#[derive(Clone, Debug)]
pub struct CacheWriteOptions {
    // Total size allocated for data in a memory mapped file.
    // This size is allocated at initialization.
    pub max_size: usize,
    // Total number of readers allowed
    pub max_readers: u32,
    // Max no of dbs
    pub max_db_size: u32,

    // Provide a path where db will be created. If nothing is provided, will default to a temp location.
    pub path: Option<PathBuf>,
}

impl Default for CacheWriteOptions {
    fn default() -> Self {
        Self {
            max_size: 1024 * 1024 * 5,
            max_readers: 10,
            max_db_size: 1000,
            path: None,
        }
    }
}

impl CacheWriteOptions {
    pub fn set_path(&mut self, path: PathBuf) {
        self.path = Some(path);
    }
}
