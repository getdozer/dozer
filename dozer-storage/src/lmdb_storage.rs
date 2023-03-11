use crate::errors::StorageError;
use dozer_types::parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use lmdb::{
    Database, DatabaseFlags, Environment, EnvironmentFlags, RoCursor, RoTransaction, RwCursor,
    RwTransaction, Transaction, WriteFlags,
};
use std::fs;
use std::path::Path;
use std::sync::Arc;

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
            flags: EnvironmentFlags::empty(),
        }
    }
}

pub trait CreateDatabase {
    fn create_database(
        &mut self,
        name: Option<&str>,
        create_flags: Option<DatabaseFlags>,
    ) -> Result<Database, StorageError>;
}

#[derive(Debug)]
/// This is a safe wrapper around `lmdb::Environment` that is opened with `NO_TLS` and `NO_LOCK`.
///
/// All write related methods that use `Environment` take `&mut self` to avoid race between transactions.
pub struct LmdbEnvironmentManager {
    inner: Environment,
}

impl CreateDatabase for LmdbEnvironmentManager {
    /// Opens a database, creating it if it doesn't exist and `create_flags` is `Some`.
    fn create_database(
        &mut self,
        name: Option<&str>,
        create_flags: Option<DatabaseFlags>,
    ) -> Result<Database, StorageError> {
        if let Some(flags) = create_flags {
            Ok(self.inner.create_db(name, flags)?)
        } else {
            Ok(self.inner.open_db(name)?)
        }
    }
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

    pub fn create(
        base_path: &Path,
        name: &str,
        options: LmdbEnvironmentOptions,
    ) -> Result<Self, StorageError> {
        let full_path = base_path.join(Path::new(name));

        let mut builder = Environment::new();
        builder.set_max_dbs(options.max_dbs);
        builder.set_map_size(options.max_map_sz);
        builder.set_max_readers(options.max_readers);
        builder.set_flags(
            options.flags
                | EnvironmentFlags::NO_SUB_DIR
                | EnvironmentFlags::NO_TLS
                | EnvironmentFlags::NO_LOCK,
        );

        let env = builder.open(&full_path)?;
        Ok(LmdbEnvironmentManager { inner: env })
    }

    pub fn create_txn(self) -> Result<SharedTransaction, StorageError> {
        Ok(SharedTransaction(Arc::new(RwLock::new(
            LmdbExclusiveTransaction::new(self.inner)?,
        ))))
    }

    pub fn begin_ro_txn(&self) -> Result<RoTransaction, StorageError> {
        Ok(self.inner.begin_ro_txn()?)
    }

    pub fn begin_rw_txn(&mut self) -> Result<RwTransaction, StorageError> {
        Ok(self.inner.begin_rw_txn()?)
    }
}

#[derive(Debug, Clone)]
pub struct SharedTransaction(Arc<RwLock<LmdbExclusiveTransaction>>);

impl SharedTransaction {
    pub fn try_unwrap(this: SharedTransaction) -> Result<LmdbExclusiveTransaction, Self> {
        Arc::try_unwrap(this.0)
            .map(|lock| lock.into_inner())
            .map_err(Self)
    }

    pub fn write(&self) -> RwLockWriteGuard<LmdbExclusiveTransaction> {
        self.0.write()
    }

    pub fn read(&self) -> RwLockReadGuard<LmdbExclusiveTransaction> {
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

impl CreateDatabase for LmdbExclusiveTransaction {
    /// If this method fails, following calls to `self` will panic.
    fn create_database(
        &mut self,
        name: Option<&str>,
        create_flags: Option<DatabaseFlags>,
    ) -> Result<Database, StorageError> {
        // SAFETY: This transaction is exclusive and commits immediately.
        let db = unsafe {
            if let Some(flags) = create_flags {
                self.txn_mut().create_db(name, flags)?
            } else {
                self.txn_mut().open_db(name)?
            }
        };
        self.commit_and_renew()?;
        Ok(db)
    }
}

impl LmdbExclusiveTransaction {
    pub fn new(env: Environment) -> Result<Self, StorageError> {
        let inner = env.begin_rw_txn()?;
        // SAFETY:
        // - `inner` does not reference data in `env`, it only has to be outlived by `env`.
        // - When we return `inner` to outside, it's always bound to lifetime of `self`, so no one can observe its `'static` lifetime.
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

    pub fn txn(&self) -> &RwTransaction {
        self.inner.as_ref().expect(PANIC_MESSAGE)
    }

    pub fn txn_mut<'a>(&'a mut self) -> &mut RwTransaction {
        // SAFETY:
        // - Only lifetime is transmuted.
        // - `RwTransaction`'s actual lifetime is `'a`, which is the lifetime of the environment.
        unsafe {
            std::mem::transmute::<&'a mut RwTransaction<'static>, &'a mut RwTransaction<'a>>(
                self.inner.as_mut().expect(PANIC_MESSAGE),
            )
        }
    }

    #[inline]
    pub fn put(&mut self, db: Database, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.inner
            .as_mut()
            .expect(PANIC_MESSAGE)
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
