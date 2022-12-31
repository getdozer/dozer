use crate::storage::errors::StorageError;
use crate::storage::errors::StorageError::InternalDbError;
use dozer_types::parking_lot::RwLock;
use libc::size_t;
use lmdb::{
    Database, DatabaseFlags, Environment, EnvironmentFlags, RoCursor, RwCursor, RwTransaction,
    Transaction, WriteFlags,
};
use lmdb_sys::{mdb_set_compare, MDB_cmp_func, MDB_SUCCESS};
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::Arc;

const DEFAULT_MAX_DBS: u32 = 256;
const DEFAULT_MAX_READERS: u32 = 256;
const DEFAULT_MAX_MAP_SZ: size_t = 1024 * 1024 * 1024;

pub struct LmdbEnvironmentManager {
    inner: Environment,
}

impl LmdbEnvironmentManager {
    pub fn exists(path: &Path, name: &str) -> bool {
        let full_path = path.join(Path::new(name));
        Path::exists(full_path.as_path())
    }

    pub fn remove(path: &Path, name: &str) {
        let full_path = path.join(Path::new(name));
        let _ = fs::remove_file(full_path);
    }

    pub fn create(base_path: &Path, name: &str) -> Result<Self, StorageError> {
        let full_path = base_path.join(Path::new(name));

        let mut builder = Environment::new();
        builder.set_max_dbs(DEFAULT_MAX_DBS);
        builder.set_map_size(DEFAULT_MAX_MAP_SZ);
        builder.set_max_readers(DEFAULT_MAX_READERS);
        builder.set_flags(
            EnvironmentFlags::NO_SUB_DIR | EnvironmentFlags::NO_TLS | EnvironmentFlags::NO_LOCK,
        );

        let env = builder.open(&full_path).map_err(InternalDbError)?;
        Ok(LmdbEnvironmentManager { inner: env })
    }

    pub fn create_txn(self) -> Result<SharedTransaction, StorageError> {
        Ok(SharedTransaction::new(LmdbExclusiveTransaction::new(
            self.inner,
        )?))
    }

    pub fn open_database(&mut self, name: &str, dup_keys: bool) -> Result<Database, StorageError> {
        let mut flags = DatabaseFlags::default();
        if dup_keys {
            flags |= DatabaseFlags::DUP_SORT;
        }
        let db = self
            .inner
            .create_db(Some(name), flags)
            .map_err(InternalDbError)?;
        Ok(db)
    }

    pub fn set_comparator(
        &mut self,
        db: Database,
        comparator: MDB_cmp_func,
    ) -> Result<(), StorageError> {
        let txn = self.inner.begin_ro_txn()?;
        unsafe {
            assert_eq!(
                mdb_set_compare(txn.txn(), db.dbi(), comparator),
                MDB_SUCCESS
            );
        }
        txn.commit().map_err(InternalDbError)
    }
}

#[derive(Debug, Clone)]
pub struct SharedTransaction(Arc<RwLock<LmdbExclusiveTransaction>>);

impl SharedTransaction {
    fn new(inner: LmdbExclusiveTransaction) -> Self {
        Self(Arc::new(RwLock::new(inner)))
    }

    pub fn try_unwrap(this: SharedTransaction) -> Result<LmdbExclusiveTransaction, Self> {
        Arc::try_unwrap(this.0)
            .map(|lock| lock.into_inner())
            .map_err(Self)
    }

    pub fn write(&self) -> impl DerefMut<Target = LmdbExclusiveTransaction> + '_ {
        self.0.write()
    }

    pub fn read(&self) -> impl Deref<Target = LmdbExclusiveTransaction> + '_ {
        self.0.read()
    }
}

// SAFETY:
// - `SharedTransaction` can only be created from `LmdbEnvironmentManager::create_txn`.
// - `LmdbEnvironmentManager` is opened with `NO_TLS` and `NO_LOCK`.
// - Inner `lmdb::RwTransaction` is protected by `RwLock`.
unsafe impl Send for SharedTransaction {}
unsafe impl Sync for SharedTransaction {}

#[derive(Debug)]
pub struct LmdbExclusiveTransaction {
    inner: Option<RwTransaction<'static>>,
    env: Environment,
}

const PANIC_MESSAGE: &str =
    "LmdbExclusiveTransaction cannot be used after `commit_and_renew` fails.";

impl LmdbExclusiveTransaction {
    pub fn new(env: Environment) -> Result<Self, StorageError> {
        let inner = env.begin_rw_txn()?;
        // SAFETY:
        // - `inner` does not reference data in `env`, it only has to be outlived by `env`.
        // - We never expose `inner` to outside, so no one can observe its `'static` lifetime.
        // - `inner` is dropped before `env`, guaranteed by `Rust` drop order.
        let inner =
            unsafe { std::mem::transmute::<RwTransaction<'_>, RwTransaction<'static>>(inner) };
        Ok(Self {
            inner: Some(inner),
            env,
        })
    }

    /// If this method fails, following calls to `self` will panic.
    pub fn commit_and_renew(&mut self) -> Result<(), StorageError> {
        self.inner.take().expect(PANIC_MESSAGE).commit()?;
        let inner = self.env.begin_rw_txn()?;
        // SAFETY: Same as `new`.
        let inner =
            unsafe { std::mem::transmute::<RwTransaction<'_>, RwTransaction<'static>>(inner) };
        self.inner = Some(inner);
        Ok(())
    }

    #[inline]
    pub fn put(&mut self, db: Database, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.inner
            .as_mut()
            .expect(PANIC_MESSAGE)
            .put(db, &key, &value, WriteFlags::default())
            .map_err(InternalDbError)
    }

    #[inline]
    pub fn del(
        &mut self,
        db: Database,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<bool, StorageError> {
        match self
            .inner
            .as_mut()
            .expect(PANIC_MESSAGE)
            .del(db, &key, value)
        {
            Ok(()) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    #[inline]
    pub fn open_cursor(&mut self, db: Database) -> Result<RwCursor, StorageError> {
        let cursor = self
            .inner
            .as_mut()
            .expect(PANIC_MESSAGE)
            .open_rw_cursor(db)?;
        Ok(cursor)
    }

    #[inline]
    pub fn get(&self, db: Database, key: &[u8]) -> Result<Option<&[u8]>, StorageError> {
        match self.inner.as_ref().expect(PANIC_MESSAGE).get(db, &key) {
            Ok(value) => Ok(Some(value)),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    #[inline]
    pub fn open_ro_cursor(&self, db: Database) -> Result<RoCursor, StorageError> {
        let cursor = self
            .inner
            .as_ref()
            .expect(PANIC_MESSAGE)
            .open_ro_cursor(db)?;
        Ok(cursor)
    }
}
