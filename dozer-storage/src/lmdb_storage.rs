use crate::errors::StorageError;
use dozer_types::log::error;
use lmdb::{
    Database, DatabaseFlags, Environment, EnvironmentFlags, RoCursor, RoTransaction, RwCursor,
    RwTransaction, Transaction, WriteFlags,
};
use std::path::Path;
use std::sync::Arc;
use std::thread::ThreadId;
use std::{fs, thread};

const DEFAULT_MAX_DBS: u32 = 256;
const DEFAULT_MAX_READERS: u32 = 256;
const DEFAULT_MAX_MAP_SZ: usize = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct LmdbEnvironmentOptions {
    pub max_dbs: u32,
    pub max_readers: u32,
    pub max_map_sz: usize,
    pub flags: lmdb::EnvironmentFlags,
}

impl LmdbEnvironmentOptions {
    pub fn new(
        max_dbs: u32,
        max_readers: u32,
        max_map_sz: usize,
        flags: lmdb::EnvironmentFlags,
    ) -> Self {
        LmdbEnvironmentOptions {
            max_dbs,
            max_readers,
            max_map_sz,
            flags,
        }
    }
}

impl Default for LmdbEnvironmentOptions {
    fn default() -> Self {
        LmdbEnvironmentOptions {
            max_dbs: DEFAULT_MAX_DBS,
            max_readers: DEFAULT_MAX_READERS,
            max_map_sz: DEFAULT_MAX_MAP_SZ,
            flags: EnvironmentFlags::NO_TLS,
        }
    }
}

pub trait LmdbEnvironment {
    fn env(&self) -> &Environment;

    fn open_database(&self, name: Option<&str>) -> Result<Database, StorageError> {
        self.env().open_db(name).map_err(Into::into)
    }

    fn begin_txn(&self) -> Result<RoTransaction<'_>, StorageError> {
        self.env().begin_ro_txn().map_err(Into::into)
    }
}

#[derive(Debug)]
pub struct LmdbEnvironmentManager;

impl LmdbEnvironmentManager {
    pub fn exists(path: &Path, name: &str) -> bool {
        let full_path = path.join(Path::new(name));
        Path::exists(full_path.as_path())
    }

    pub fn remove(path: &Path, name: &str) {
        let full_path = path.join(Path::new(name));
        let _ = fs::remove_file(full_path);
    }

    pub fn create_rw(
        base_path: &Path,
        name: &str,
        options: LmdbEnvironmentOptions,
    ) -> Result<RwLmdbEnvironment, StorageError> {
        let page_size = page_size::get();
        if options.max_map_sz == 0 || options.max_map_sz % page_size != 0 {
            return Err(StorageError::BadPageSize {
                map_size: options.max_map_sz,
                page_size,
            });
        }
        if options.flags.contains(EnvironmentFlags::READ_ONLY) {
            return Err(StorageError::InvalidArgument(
                "Cannot create a read-write environment with READ_ONLY flag.".to_string(),
            ));
        }
        let env = Self::open_env(base_path, name, options)?;
        RwLmdbEnvironment::new(env)
    }

    pub fn create_ro(
        base_path: &Path,
        name: &str,
        mut options: LmdbEnvironmentOptions,
    ) -> Result<RoLmdbEnvironment, StorageError> {
        options.flags |= EnvironmentFlags::READ_ONLY;
        let env = Self::open_env(base_path, name, options)?;
        Ok(RoLmdbEnvironment::new(Arc::new(env)))
    }

    fn open_env(
        base_path: &Path,
        name: &str,
        options: LmdbEnvironmentOptions,
    ) -> Result<Environment, StorageError> {
        if options.flags.contains(EnvironmentFlags::NO_LOCK) {
            return Err(StorageError::InvalidArgument(
                "Cannot create an environment with NO_LOCK flag.".to_string(),
            ));
        }

        let full_path = base_path.join(Path::new(name));

        let mut builder = Environment::new();
        builder.set_max_dbs(options.max_dbs);
        builder.set_map_size(options.max_map_sz);
        builder.set_max_readers(options.max_readers);
        builder.set_flags(options.flags | EnvironmentFlags::NO_SUB_DIR);

        builder.open(&full_path).map_err(Into::into)
    }
}

#[derive(Debug)]
pub struct RwLmdbEnvironment {
    inner: Option<(RwTransaction<'static>, ThreadId)>,
    env: Arc<Environment>,
}

impl LmdbEnvironment for RwLmdbEnvironment {
    fn env(&self) -> &Environment {
        &self.env
    }
}

impl RwLmdbEnvironment {
    fn new(env: Environment) -> Result<Self, StorageError> {
        Ok(Self {
            inner: None,
            env: Arc::new(env),
        })
    }

    /// Shares this read-write environment with a read-only environment.
    pub fn share(&self) -> RoLmdbEnvironment {
        RoLmdbEnvironment::new(self.env.clone())
    }

    pub fn create_database(
        &mut self,
        name: Option<&str>,
        flags: DatabaseFlags,
    ) -> Result<Database, StorageError> {
        // SAFETY:
        // - `RwLmdbEnvironment` can only be constructed from an whole environment and can only share as a `RoLmdbEnvironment`.
        // - `RoLmdbEnvironment` cannot be used to open `RwTransaction`.
        // - `Environment` is never exposed directly.
        // so the transaction is unique.
        let database = unsafe { self.txn_mut()?.create_db(name, flags)? };
        self.commit()?;
        Ok(database)
    }

    pub fn commit(&mut self) -> Result<(), StorageError> {
        if let Some((txn, thread_id)) = self.inner.take() {
            let current_thread_id = thread::current().id();
            if thread_id != current_thread_id {
                self.inner = Some((txn, thread_id));
                return Err(StorageError::TransactionCommittedAcrossThread {
                    create_thread_id: thread_id,
                    commit_thread_id: current_thread_id,
                });
            }

            txn.commit()?;
        }
        Ok(())
    }

    pub fn txn_mut(&mut self) -> Result<&mut RwTransaction, StorageError> {
        if let Some((txn, _)) = self.inner.as_mut() {
            // SAFETY:
            // - Transmute txn back to its actual lifetime.
            Ok(unsafe { std::mem::transmute(txn) })
        } else {
            let inner = self.env.begin_rw_txn()?;
            // SAFETY:
            // - `inner` does not reference data in `env`, it only has to be outlived by `env`.
            // - When we return `inner` to outside, it's always bound to lifetime of `self`, so no one can observe its `'static` lifetime.
            // - `inner` is dropped before `env`, guaranteed by `Rust` drop order.
            let inner =
                unsafe { std::mem::transmute::<RwTransaction<'_>, RwTransaction<'static>>(inner) };
            self.inner = Some((inner, thread::current().id()));
            self.txn_mut()
        }
    }

    #[inline]
    pub fn put(&mut self, db: Database, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.txn_mut()?
            .put(db, &key, &value, WriteFlags::default())
            .map_err(Into::into)
    }

    #[inline]
    pub fn del(
        &mut self,
        db: Database,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<bool, StorageError> {
        match self.txn_mut()?.del(db, &key, value) {
            Ok(()) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    #[inline]
    pub fn open_cursor(&mut self, db: Database) -> Result<RwCursor, StorageError> {
        let cursor = self.txn_mut()?.open_rw_cursor(db)?;
        Ok(cursor)
    }

    #[inline]
    pub fn get(&mut self, db: Database, key: &[u8]) -> Result<Option<&[u8]>, StorageError> {
        match self.txn_mut()?.get(db, &key) {
            Ok(value) => Ok(Some(value)),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    #[inline]
    pub fn open_ro_cursor(&mut self, db: Database) -> Result<RoCursor, StorageError> {
        let cursor = self.txn_mut()?.open_ro_cursor(db)?;
        Ok(cursor)
    }
}

impl Drop for RwLmdbEnvironment {
    fn drop(&mut self) {
        if let Some((_, thread_id)) = self.inner.as_ref() {
            let current_thread_id = thread::current().id();
            if thread_id != &current_thread_id {
                error!(
                    "Transaction dropped across thread, create thread id: {:?}, drop thread id: {:?}. This may cause deadlock.",
                    thread_id, current_thread_id
                );
            }
        }
    }
}

// Safety: We check that the `RwTransaction` is used in the same thread at runtime.
unsafe impl Send for RwLmdbEnvironment {}
unsafe impl Sync for RwLmdbEnvironment {}

#[derive(Debug, Clone)]
pub struct RoLmdbEnvironment(Arc<Environment>);

impl LmdbEnvironment for RoLmdbEnvironment {
    fn env(&self) -> &Environment {
        &self.0
    }
}

impl RoLmdbEnvironment {
    pub fn new(env: Arc<Environment>) -> Self {
        Self(env)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn test_create_then_open_database() {
        let temp_dir = TempDir::new("test").unwrap();
        let name = "test";
        let mut rw_env = LmdbEnvironmentManager::create_rw(
            temp_dir.path(),
            name,
            LmdbEnvironmentOptions::default(),
        )
        .unwrap();

        let db_name = Some("db");
        rw_env
            .create_database(db_name, DatabaseFlags::empty())
            .unwrap();

        let ro_env = LmdbEnvironmentManager::create_ro(
            temp_dir.path(),
            name,
            LmdbEnvironmentOptions::default(),
        )
        .unwrap();
        ro_env.open_database(db_name).unwrap();
    }
}
