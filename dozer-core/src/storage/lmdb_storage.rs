use crate::storage::common::{
    Database, Environment, EnvironmentManager, RenewableRwTransaction, RoCursor, RwCursor,
};
use crate::storage::errors::StorageError;
use crate::storage::errors::StorageError::InternalDbError;
use crate::storage::lmdb_sys::{
    Cursor as LmdbCursor, CursorPutOptions as LmdbCursorPutOptions, Database as LmdbDatabase,
    DatabaseOptions as LmdbDatabaseOptions, EnvOptions, Environment as LmdbEnvironment,
    PutOptions as LmdbPutOptions, Transaction as LmdbTransaction,
};
use libc::size_t;
use std::path::Path;

const DEFAULT_MAX_DBS: u32 = 256;
const DEFAULT_MAX_READERS: u32 = 256;
const DEFAULT_MAX_MAP_SZ: size_t = 1024 * 1024 * 1024;

pub struct LmdbEnvironmentManager {
    inner: LmdbEnvironment,
    dbs: Vec<LmdbDatabase>,
}

impl LmdbEnvironmentManager {
    pub fn create(
        base_path: &Path,
        name: &str,
    ) -> Result<Box<dyn EnvironmentManager>, StorageError> {
        let full_path = base_path.join(Path::new(name));

        let mut env_opt = EnvOptions::default();

        env_opt.max_dbs = Some(DEFAULT_MAX_DBS);
        env_opt.map_size = Some(DEFAULT_MAX_MAP_SZ);
        env_opt.max_readers = Some(DEFAULT_MAX_READERS);
        env_opt.writable_mem_map = false;
        env_opt.no_subdir = true;
        env_opt.no_thread_local_storage = true;
        env_opt.no_locking = true;

        let env = LmdbEnvironment::new(full_path.to_str().unwrap().to_string(), env_opt)
            .map_err(InternalDbError)?;
        Ok(Box::new(LmdbEnvironmentManager {
            inner: env,
            dbs: Vec::new(),
        }))
    }
}

impl EnvironmentManager for LmdbEnvironmentManager {
    fn as_environment(&mut self) -> &mut dyn Environment {
        self
    }

    fn create_txn(&mut self) -> Result<Box<dyn RenewableRwTransaction>, StorageError> {
        let tx = self.inner.tx_begin(false).map_err(InternalDbError)?;
        Ok(Box::new(LmdbExclusiveTransaction::new(
            self.inner.clone(),
            tx,
            self.dbs.clone(),
        )))
    }
}

impl Environment for LmdbEnvironmentManager {
    fn open_database(&mut self, name: &str, dup_keys: bool) -> Result<Database, StorageError> {
        let mut tx = self.inner.tx_begin(false).map_err(InternalDbError)?;
        let mut db_opts = LmdbDatabaseOptions::default();
        db_opts.create = true;
        db_opts.allow_duplicate_keys = dup_keys;
        let db = tx
            .open_database(name.to_string(), db_opts)
            .map_err(InternalDbError)?;
        tx.commit().map_err(InternalDbError)?;
        self.dbs.push(db);
        Ok(Database::new(self.dbs.len() - 1))
    }
}

struct LmdbExclusiveTransaction {
    env: LmdbEnvironment,
    inner: LmdbTransaction,
    dbs: Vec<LmdbDatabase>,
}

impl LmdbExclusiveTransaction {
    pub fn new(env: LmdbEnvironment, inner: LmdbTransaction, dbs: Vec<LmdbDatabase>) -> Self {
        Self { env, inner, dbs }
    }
}

impl RenewableRwTransaction for LmdbExclusiveTransaction {
    fn commit_and_renew(&mut self) -> Result<(), StorageError> {
        self.inner.commit().map_err(InternalDbError)?;
        self.inner = self.env.tx_begin(false)?;
        Ok(())
    }

    fn abort_and_renew(&mut self) -> Result<(), StorageError> {
        self.inner.abort().map_err(InternalDbError)?;
        self.inner = self.env.tx_begin(false)?;
        Ok(())
    }

    #[inline]
    fn put(&mut self, db: &Database, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.inner
            .put(&self.dbs[db.id], key, value, LmdbPutOptions::default())
            .map_err(InternalDbError)
    }

    #[inline]
    fn del(
        &mut self,
        db: &Database,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<bool, StorageError> {
        self.inner
            .del(&self.dbs[db.id], key, value)
            .map_err(InternalDbError)
    }

    #[inline]
    fn open_cursor(&self, db: &Database) -> Result<Box<dyn RwCursor>, StorageError> {
        let cursor = self.inner.open_cursor(&self.dbs[db.id])?;
        Ok(Box::new(ReaderWriterCursor::new(cursor)))
    }

    #[inline]
    fn get(&self, db: &Database, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self
            .inner
            .get(&self.dbs[db.id], key)
            .map_err(InternalDbError)?
            .map(Vec::from))
    }

    #[inline]
    fn open_ro_cursor(&self, db: &Database) -> Result<Box<dyn RoCursor>, StorageError> {
        let cursor = self.inner.open_cursor(&self.dbs[db.id])?;
        Ok(Box::new(ReaderWriterCursor::new(cursor)))
    }
}

pub struct ReaderWriterCursor {
    inner: LmdbCursor,
}

impl ReaderWriterCursor {
    pub fn new(inner: LmdbCursor) -> Self {
        Self { inner }
    }
}

impl RoCursor for ReaderWriterCursor {
    #[inline]
    fn seek_gte(&self, key: &[u8]) -> Result<bool, StorageError> {
        self.inner.seek_gte(key).map_err(InternalDbError)
    }

    #[inline]
    fn seek(&self, key: &[u8]) -> Result<bool, StorageError> {
        self.inner.seek(key).map_err(InternalDbError)
    }

    #[inline]
    fn seek_partial(&self, key: &[u8]) -> Result<bool, StorageError> {
        self.inner.seek_partial(key).map_err(InternalDbError)
    }

    #[inline]
    fn read(&self) -> Result<Option<(&[u8], &[u8])>, StorageError> {
        self.inner.read().map_err(InternalDbError)
    }

    #[inline]
    fn next(&self) -> Result<bool, StorageError> {
        self.inner.next().map_err(InternalDbError)
    }

    #[inline]
    fn prev(&self) -> Result<bool, StorageError> {
        self.inner.prev().map_err(InternalDbError)
    }
}

impl RwCursor for ReaderWriterCursor {
    #[inline]
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.inner
            .put(key, value, &LmdbCursorPutOptions::default())
            .map_err(InternalDbError)
    }
}
