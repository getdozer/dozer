#![allow(clippy::type_complexity)]
use libc::{c_int, c_uint, c_void, size_t, EACCES, EAGAIN, EINVAL, EIO, ENOENT, ENOMEM, ENOSPC};
use lmdb_sys::{
    mdb_cursor_close, mdb_cursor_get, mdb_cursor_open, mdb_cursor_put, mdb_dbi_close, mdb_dbi_open,
    mdb_del, mdb_env_close, mdb_env_create, mdb_env_open, mdb_env_set_mapsize, mdb_env_set_maxdbs,
    mdb_env_set_maxreaders, mdb_get, mdb_put, mdb_txn_abort, mdb_txn_begin, mdb_txn_commit,
    MDB_cursor, MDB_cursor_op, MDB_dbi, MDB_env, MDB_txn, MDB_val, MDB_APPEND, MDB_APPENDDUP,
    MDB_CREATE, MDB_CURRENT, MDB_DBS_FULL, MDB_DUPFIXED, MDB_DUPSORT, MDB_FIRST, MDB_GET_CURRENT,
    MDB_INTEGERKEY, MDB_INVALID, MDB_MAP_FULL, MDB_MAP_RESIZED, MDB_NEXT, MDB_NODUPDATA,
    MDB_NOLOCK, MDB_NOMETASYNC, MDB_NOOVERWRITE, MDB_NOSUBDIR, MDB_NOSYNC, MDB_NOTFOUND, MDB_NOTLS,
    MDB_PANIC, MDB_PREV, MDB_RDONLY, MDB_READERS_FULL, MDB_RESERVE, MDB_SET, MDB_SET_RANGE,
    MDB_TXN_FULL, MDB_VERSION_MISMATCH, MDB_WRITEMAP,
};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ptr::addr_of_mut;
use std::sync::Arc;
use std::{ptr, slice};
use unixstring::UnixString;

#[derive(Debug, Clone)]
pub struct LmdbError {
    pub err_no: i32,
    pub err_str: String,
}

impl Display for LmdbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("LMDB Error: {} - {}", self.err_no, self.err_str.as_str()).as_str())
    }
}

impl Error for LmdbError {}

impl LmdbError {
    pub fn new(err_no: i32, err_str: String) -> Self {
        Self { err_no, err_str }
    }
}

struct EnvPtr {
    env: *mut MDB_env,
}

impl EnvPtr {
    pub fn new(env: *mut MDB_env) -> Self {
        Self { env }
    }
}

impl Drop for EnvPtr {
    fn drop(&mut self) {
        unsafe {
            mdb_env_close(self.env);
        }
    }
}

struct TxnPtr {
    txn: *mut MDB_txn,
    read_only: bool,
    active: bool,
}

impl TxnPtr {
    pub fn new(txn: *mut MDB_txn, read_only: bool) -> Self {
        Self {
            txn,
            read_only,
            active: true,
        }
    }
}

impl Drop for TxnPtr {
    fn drop(&mut self) {
        if self.active {}
    }
}

struct DbPtr {
    env: Arc<EnvPtr>,
    db: MDB_dbi,
}

impl DbPtr {
    pub fn new(env: Arc<EnvPtr>, db: MDB_dbi) -> Self {
        Self { env, db }
    }
}

impl Drop for DbPtr {
    fn drop(&mut self) {
        unsafe {
            mdb_dbi_close(self.env.env, self.db);
        }
    }
}

struct CursorPtr {
    cursor: *mut MDB_cursor,
}

impl CursorPtr {
    pub fn new(cursor: *mut MDB_cursor) -> Self {
        Self { cursor }
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        unsafe {
            mdb_cursor_close(self.cursor.cursor);
        }
    }
}

#[inline]
fn mdb_val(v: &[u8]) -> MDB_val {
    MDB_val {
        mv_size: v.len(),
        mv_data: v.as_ptr() as *mut c_void,
    }
}

#[inline]
fn mdb_null_val() -> MDB_val {
    MDB_val {
        mv_size: 0,
        mv_data: ptr::null_mut(),
    }
}

macro_rules! set_flags {
    ($src: expr, $flag_var: expr, $flag: expr) => {
        if $src {
            $flag_var |= $flag;
        }
    };
}

/***********************************************************************************
 Environment
***********************************************************************************/

pub struct Environment {
    env_ptr: Arc<EnvPtr>,
}

unsafe impl Send for Environment {}
unsafe impl Sync for Environment {}

impl Clone for Environment {
    fn clone(&self) -> Self {
        Environment {
            env_ptr: self.env_ptr.clone(),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct EnvOptions {
    pub map_size: Option<size_t>,
    pub max_dbs: Option<u32>,
    pub max_readers: Option<u32>,
    pub no_sync: bool,
    pub no_meta_sync: bool,
    pub no_subdir: bool,
    pub writable_mem_map: bool,
    pub no_locking: bool,
    pub no_thread_local_storage: bool,
}

impl EnvOptions {
    pub fn default() -> Self {
        Self {
            map_size: None,
            max_dbs: None,
            max_readers: None,
            no_sync: false,
            no_meta_sync: false,
            no_subdir: false,
            writable_mem_map: false,
            no_locking: false,
            no_thread_local_storage: false,
        }
    }
}

impl Environment {
    pub fn new(path: String, o: EnvOptions) -> Result<Environment, LmdbError> {
        unsafe {
            let mut env_ptr: *mut MDB_env = ptr::null_mut();

            let r = mdb_env_create(addr_of_mut!(env_ptr));
            match r {
                MDB_VERSION_MISMATCH => { return Err(LmdbError::new(r, "The version of the LMDB library doesn't match the version that created the database environment".to_string())) }
                MDB_INVALID => { return Err(LmdbError::new(r, "The environment file headers are corrupted".to_string())) }
                ENOENT => { return Err(LmdbError::new(r, "The directory specified by the path parameter doesn't exist".to_string())) }
                EACCES => { return Err(LmdbError::new(r, "The user didn't have permission to access the environment files".to_string())) }
                EAGAIN => { return Err(LmdbError::new(r, "The environment was locked by another process".to_string())) }
                x if x != 0 => { return Err(LmdbError::new(r, "Unknown error".to_string())) }
                _ => {}
            }

            let mut flags: c_uint = 0;

            if o.map_size.is_some() && mdb_env_set_mapsize(env_ptr, o.map_size.unwrap()) != 0 {
                return Err(LmdbError::new(r, "Invalid map size specified".to_string()));
            }
            if o.max_dbs.is_some() && mdb_env_set_maxdbs(env_ptr, o.max_dbs.unwrap()) != 0 {
                return Err(LmdbError::new(r, "Invalid map size specified".to_string()));
            }
            if o.max_readers.is_some()
                && mdb_env_set_maxreaders(env_ptr, o.max_readers.unwrap()) != 0
            {
                return Err(LmdbError::new(
                    r,
                    "Invalid max readers size specified".to_string(),
                ));
            }
            if o.no_sync {
                flags |= MDB_NOSYNC;
            }
            if o.no_meta_sync {
                flags |= MDB_NOMETASYNC;
            }
            if o.no_subdir {
                flags |= MDB_NOSUBDIR;
            }
            if o.writable_mem_map {
                flags |= MDB_WRITEMAP;
            }
            if o.no_locking {
                flags |= MDB_NOLOCK;
            }
            if o.no_thread_local_storage {
                flags |= MDB_NOTLS;
            }

            let r = mdb_env_open(
                env_ptr,
                UnixString::from_string(path).unwrap().as_ptr(),
                flags,
                0o664,
            );
            match r {
                MDB_VERSION_MISMATCH => { return Err(LmdbError::new(r, "The version of the LMDB library doesn't match the version that created the database environment".to_string())) }
                MDB_INVALID => { return Err(LmdbError::new(r, "The environment file headers are corrupted".to_string())) }
                ENOENT => { return Err(LmdbError::new(r, "The directory specified by the path parameter doesn't exist".to_string())) }
                EACCES => { return Err(LmdbError::new(r, "The user didn't have permission to access the environment files".to_string())) }
                EAGAIN => { return Err(LmdbError::new(r, "The environment was locked by another process".to_string())) }
                x if x != 0 => { return Err(LmdbError::new(r, "Unknown error".to_string())) }
                _ => {}
            }

            Ok(Environment {
                env_ptr: Arc::new(EnvPtr::new(env_ptr)),
            })
        }
    }

    pub fn tx_begin(&mut self, read_only: bool) -> Result<Transaction, LmdbError> {
        Transaction::begin(self, read_only)
    }
}

/***********************************************************************************
 Transaction
***********************************************************************************/

pub struct Transaction {
    env: Arc<EnvPtr>,
    txn: Arc<TxnPtr>,
    parent: Option<Arc<TxnPtr>>,
}

unsafe impl Send for Transaction {}
unsafe impl Sync for Transaction {}

impl Transaction {
    pub fn begin(env: &Environment, read_only: bool) -> Result<Transaction, LmdbError> {
        unsafe {
            let mut txn_ptr: *mut MDB_txn = ptr::null_mut();

            let r = mdb_txn_begin(
                env.env_ptr.env,
                ptr::null_mut(),
                if read_only { MDB_RDONLY } else { 0 },
                addr_of_mut!(txn_ptr),
            );
            match r {
                MDB_PANIC => { return Err(LmdbError::new(r, "A fatal error occurred earlier and the environment must be shut down".to_string())) }
                MDB_MAP_RESIZED => { return Err(LmdbError::new(r, "Another process wrote data beyond this MDB_env's mapsize and this environment's map must be resized as well. See mdb_env_set_mapsize()".to_string())) }
                MDB_READERS_FULL => { return Err(LmdbError::new(r, "A read-only transaction was requested and the reader lock table is full. See mdb_env_set_maxreaders()".to_string())) }
                ENOMEM => { return Err(LmdbError::new(r, "Out of Memory".to_string())) }
                x if x != 0 => { return Err(LmdbError::new(r, "Unknown error".to_string())) }
                _ => {}
            }

            Ok(Transaction {
                env: env.env_ptr.clone(),
                txn: Arc::new(TxnPtr::new(txn_ptr, read_only)),
                parent: None,
            })
        }
    }

    pub fn open_database(&self, name: String, o: DatabaseOptions) -> Result<Database, LmdbError> {
        unsafe {
            let mut dbi: MDB_dbi = 0;

            let mut opt_flags: c_uint = 0;

            if o.create {
                opt_flags |= MDB_CREATE
            }
            if o.allow_duplicate_keys {
                opt_flags |= MDB_DUPSORT
            }
            if o.integer_keys {
                opt_flags |= MDB_INTEGERKEY
            }
            if o.fixed_key_size {
                opt_flags |= MDB_DUPFIXED
            }

            let r = mdb_dbi_open(
                self.txn.txn,
                UnixString::from_string(name).unwrap().as_ptr(),
                opt_flags,
                addr_of_mut!(dbi),
            );

            match r {
                MDB_NOTFOUND => { return Err(LmdbError::new(r, "The specified database doesn't exist in the environment and MDB_CREATE was not specified".to_string())) }
                MDB_DBS_FULL => { return Err(LmdbError::new(r, "Too many databases have been opened. See mdb_env_set_maxdbs()".to_string())) }
                x if x != 0 => { return Err(LmdbError::new(r, "Unknown error".to_string())) }
                _ => {}
            }

            Ok(Database {
                dbi: Arc::new(DbPtr::new(self.env.clone(), dbi)),
            })
        }
    }

    pub fn child(&self) -> Result<Transaction, LmdbError> {
        unsafe {
            let mut txn_ptr: *mut MDB_txn = ptr::null_mut();

            let r = mdb_txn_begin(
                self.env.env,
                self.txn.txn,
                if self.txn.read_only { MDB_RDONLY } else { 0 },
                addr_of_mut!(txn_ptr),
            );
            match r {
                MDB_PANIC => { return Err(LmdbError::new(r, "A fatal error occurred earlier and the environment must be shut down".to_string())) }
                MDB_MAP_RESIZED => { return Err(LmdbError::new(r, "Another process wrote data beyond this MDB_env's mapsize and this environment's map must be resized as well. See mdb_env_set_mapsize()".to_string())) }
                MDB_READERS_FULL => { return Err(LmdbError::new(r, "A read-only transaction was requested and the reader lock table is full. See mdb_env_set_maxreaders()".to_string())) }
                ENOMEM => { return Err(LmdbError::new(r, "Out of Memory".to_string())) }
                -30782 => { return Err(LmdbError::new(r, "Bad transaction".to_string())) }
                x if x != 0 => { return Err(LmdbError::new(r, "Unknown error".to_string())) }
                _ => {}
            }

            Ok(Transaction {
                env: self.env.clone(),
                txn: Arc::new(TxnPtr::new(txn_ptr, self.txn.read_only)),
                parent: Some(self.txn.clone()),
            })
        }
    }

    pub fn commit(&mut self) -> Result<(), LmdbError> {
        unsafe {
            let r = mdb_txn_commit(self.txn.txn);
            match r {
                EINVAL => {
                    return Err(LmdbError::new(
                        r,
                        "An invalid parameter was specified".to_string(),
                    ))
                }
                ENOSPC => return Err(LmdbError::new(r, "No more space on disk".to_string())),
                EIO => {
                    return Err(LmdbError::new(
                        r,
                        "S low-level I/O error occurred while writing".to_string(),
                    ))
                }
                ENOMEM => return Err(LmdbError::new(r, "Out of memory".to_string())),
                x if x != 0 => return Err(LmdbError::new(r, "Unknown error".to_string())),
                _ => {}
            }
            Ok(())
        }
    }

    pub fn abort(&self) -> Result<(), LmdbError> {
        unsafe {
            mdb_txn_abort(self.txn.txn);
            Ok(())
        }
    }

    pub fn put(
        &mut self,
        db: &Database,
        key: &[u8],
        value: &[u8],
        o: PutOptions,
    ) -> Result<(), LmdbError> {
        unsafe {
            let mut key_data = MDB_val {
                mv_size: key.len(),
                mv_data: key.as_ptr() as *mut c_void,
            };
            let mut val_data = MDB_val {
                mv_size: value.len(),
                mv_data: value.as_ptr() as *mut c_void,
            };

            let mut opt_flags: c_uint = 0;

            if o.no_duplicates {
                opt_flags |= MDB_NODUPDATA
            }
            if o.no_overwrite {
                opt_flags |= MDB_NOOVERWRITE
            }

            let r = mdb_put(
                self.txn.txn,
                db.dbi.db,
                addr_of_mut!(key_data),
                addr_of_mut!(val_data),
                opt_flags,
            );
            match r {
                MDB_MAP_FULL => {
                    return Err(LmdbError::new(
                        r,
                        "The database is full, see mdb_env_set_mapsize()".to_string(),
                    ))
                }
                MDB_TXN_FULL => {
                    return Err(LmdbError::new(
                        r,
                        "the transaction has too many dirty pages".to_string(),
                    ))
                }
                EACCES => {
                    return Err(LmdbError::new(
                        r,
                        "An attempt was made to write in a read-only transaction".to_string(),
                    ))
                }
                EINVAL => return Err(LmdbError::new(r, "Invalid parameter".to_string())),
                x if x != 0 => return Err(LmdbError::new(r, "Unknown error".to_string())),
                _ => {}
            }

            Ok(())
        }
    }

    pub fn get(&self, db: &Database, key: &[u8]) -> Result<Option<&[u8]>, LmdbError> {
        unsafe {
            let mut key_data = MDB_val {
                mv_size: key.len(),
                mv_data: key.as_ptr() as *mut c_void,
            };
            let mut val_data = MDB_val {
                mv_size: 0,
                mv_data: ptr::null_mut(),
            };

            let r = mdb_get(
                self.txn.txn,
                db.dbi.db,
                addr_of_mut!(key_data),
                addr_of_mut!(val_data),
            );
            match r {
                MDB_NOTFOUND => return Ok(None),
                EINVAL => return Err(LmdbError::new(r, "Invalid parameter".to_string())),
                x if x != 0 => return Err(LmdbError::new(r, "Unknown error".to_string())),
                _ => {}
            }

            Ok(Some(slice::from_raw_parts(
                val_data.mv_data as *mut u8,
                val_data.mv_size as usize,
            )))
        }
    }

    pub fn del(
        &mut self,
        db: &Database,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<bool, LmdbError> {
        unsafe {
            let mut key_data = MDB_val {
                mv_size: key.len(),
                mv_data: key.as_ptr() as *mut c_void,
            };
            let val_data = value.map(|e| MDB_val {
                mv_size: e.len(),
                mv_data: e.as_ptr() as *mut c_void,
            });

            let r: c_int = match val_data {
                Some(mut v) => mdb_del(
                    self.txn.txn,
                    db.dbi.db,
                    addr_of_mut!(key_data),
                    addr_of_mut!(v),
                ),
                None => mdb_del(
                    self.txn.txn,
                    db.dbi.db,
                    addr_of_mut!(key_data),
                    ptr::null_mut(),
                ),
            };

            match r {
                MDB_NOTFOUND => return Ok(false),
                EACCES => {
                    return Err(LmdbError::new(
                        r,
                        "An attempt was made to write in a read-only transaction".to_string(),
                    ))
                }
                EINVAL => return Err(LmdbError::new(r, "Invalid parameter".to_string())),
                x if x != 0 => return Err(LmdbError::new(r, "Unknown error".to_string())),
                _ => {}
            }

            Ok(true)
        }
    }

    pub fn open_cursor(&self, db: &Database) -> Result<Cursor, LmdbError> {
        unsafe {
            let mut cur: *mut MDB_cursor = ptr::null_mut();
            let r = mdb_cursor_open(self.txn.txn, db.dbi.db, addr_of_mut!(cur));
            match r {
                EINVAL => Err(LmdbError::new(r, "Invalid parameter".to_string())),
                x if x != 0 => Err(LmdbError::new(r, "Unknown error".to_string())),
                _ => Ok(Cursor::new(
                    db.dbi.clone(),
                    self.txn.clone(),
                    Arc::new(CursorPtr::new(cur)),
                )),
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct DatabaseOptions {
    pub create: bool,
    pub allow_duplicate_keys: bool,
    pub integer_keys: bool,
    pub fixed_key_size: bool,
}

impl DatabaseOptions {
    pub fn new(
        create: bool,
        allow_duplicate_keys: bool,
        integer_keys: bool,
        fixed_key_size: bool,
    ) -> Self {
        Self {
            create,
            allow_duplicate_keys,
            integer_keys,
            fixed_key_size,
        }
    }

    pub fn default() -> Self {
        Self {
            create: true,
            allow_duplicate_keys: false,
            integer_keys: false,
            fixed_key_size: false,
        }
    }
}

/***********************************************************************************
 Database
***********************************************************************************/

pub struct Database {
    dbi: Arc<DbPtr>,
}

unsafe impl Send for Database {}
unsafe impl Sync for Database {}

impl Clone for Database {
    fn clone(&self) -> Self {
        Database {
            dbi: self.dbi.clone(),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct PutOptions {
    no_duplicates: bool,
    no_overwrite: bool,
}

impl PutOptions {
    pub fn default() -> Self {
        Self {
            no_duplicates: false,
            no_overwrite: false,
        }
    }
}

impl Database {
    pub fn open(
        env: &Environment,
        txn: &Transaction,
        name: String,
        opts: Option<DatabaseOptions>,
    ) -> Result<Database, LmdbError> {
        unsafe {
            let mut dbi: MDB_dbi = 0;

            let mut opt_flags: c_uint = 0;

            if let Some(o) = opts {
                if o.create {
                    opt_flags |= MDB_CREATE
                }
                if o.allow_duplicate_keys {
                    opt_flags |= MDB_DUPSORT
                }
                if o.integer_keys {
                    opt_flags |= MDB_INTEGERKEY
                }
                if o.fixed_key_size {
                    opt_flags |= MDB_DUPFIXED
                }
            }

            let r = mdb_dbi_open(
                txn.txn.txn,
                UnixString::from_string(name).unwrap().as_ptr(),
                opt_flags,
                addr_of_mut!(dbi),
            );

            match r {
                MDB_NOTFOUND => { return Err(LmdbError::new(r, "The specified database doesn't exist in the environment and MDB_CREATE was not specified".to_string())) }
                MDB_DBS_FULL => { return Err(LmdbError::new(r, "Too many databases have been opened. See mdb_env_set_maxdbs()".to_string())) }
                x if x != 0 => { return Err(LmdbError::new(r, "Unknown error".to_string())) }
                _ => {}
            }

            Ok(Database {
                dbi: Arc::new(DbPtr::new(env.env_ptr.clone(), dbi)),
            })
        }
    }
}

pub struct CursorPutOptions {
    pub current: bool,
    pub no_duplicate_data: bool,
    pub no_overwrite: bool,
    pub reserve: bool,
    pub append: bool,
    pub append_duplicate: bool,
}

impl CursorPutOptions {
    pub fn default() -> Self {
        Self {
            current: false,
            no_duplicate_data: false,
            no_overwrite: false,
            reserve: false,
            append: false,
            append_duplicate: false,
        }
    }
}

pub struct Cursor {
    db: Arc<DbPtr>,
    txn: Arc<TxnPtr>,
    cursor: Arc<CursorPtr>,
}

impl Cursor {
    fn new(db: Arc<DbPtr>, txn: Arc<TxnPtr>, cursor: Arc<CursorPtr>) -> Self {
        Self { db, txn, cursor }
    }

    fn internal_get_cursor_op(
        &self,
        op: MDB_cursor_op,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
    ) -> Result<Option<(&[u8], &[u8])>, LmdbError> {
        unsafe {
            let mut key_data = match key {
                Some(v) => MDB_val {
                    mv_size: v.len(),
                    mv_data: v.as_ptr() as *mut c_void,
                },
                _ => MDB_val {
                    mv_size: 0,
                    mv_data: ptr::null_mut(),
                },
            };

            let mut val_data = match val {
                Some(v) => MDB_val {
                    mv_size: v.len(),
                    mv_data: v.as_ptr() as *mut c_void,
                },
                _ => MDB_val {
                    mv_size: 0,
                    mv_data: ptr::null_mut(),
                },
            };

            let r = mdb_cursor_get(
                self.cursor.cursor,
                addr_of_mut!(key_data),
                addr_of_mut!(val_data),
                op,
            );

            match r {
                EINVAL => Err(LmdbError::new(r, "Invalid parameter".to_string())),
                MDB_NOTFOUND => Ok(None),
                x if x != 0 => Err(LmdbError::new(r, "Unknown error".to_string())),
                _ => Ok(Some((
                    slice::from_raw_parts(key_data.mv_data as *mut u8, key_data.mv_size as usize),
                    slice::from_raw_parts(val_data.mv_data as *mut u8, val_data.mv_size as usize),
                ))),
            }
        }
    }

    pub fn seek_gte(&self, key: &[u8]) -> Result<bool, LmdbError> {
        let r = self.internal_get_cursor_op(MDB_SET_RANGE, Some(key), None);
        match r {
            Ok(Some(_v)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub fn seek(&self, key: &[u8]) -> Result<bool, LmdbError> {
        let r = self.internal_get_cursor_op(MDB_SET, Some(key), None);
        match r {
            Ok(Some(_v)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub fn seek_partial(&self, key: &[u8]) -> Result<bool, LmdbError> {
        let r = self.internal_get_cursor_op(MDB_SET_RANGE, Some(key), None);
        match r {
            Ok(Some(_v)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub fn read(&self) -> Result<Option<(&[u8], &[u8])>, LmdbError> {
        self.internal_get_cursor_op(MDB_GET_CURRENT, None, None)
    }

    pub fn next(&self) -> Result<bool, LmdbError> {
        let r = self.internal_get_cursor_op(MDB_NEXT, None, None);
        match r {
            Ok(Some(_v)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub fn first(&self) -> Result<bool, LmdbError> {
        let r = self.internal_get_cursor_op(MDB_FIRST, None, None);
        match r {
            Ok(Some(_v)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub fn prev(&self) -> Result<bool, LmdbError> {
        let r = self.internal_get_cursor_op(MDB_PREV, None, None);
        match r {
            Ok(Some(_v)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8], opts: &CursorPutOptions) -> Result<(), LmdbError> {
        unsafe {
            let mut key_data = mdb_val(key);
            let mut val_data = mdb_val(value);

            let mut opt_flags: c_uint = 0;
            set_flags!(opts.current, opt_flags, MDB_CURRENT);
            set_flags!(opts.no_duplicate_data, opt_flags, MDB_NODUPDATA);
            set_flags!(opts.no_overwrite, opt_flags, MDB_NOOVERWRITE);
            set_flags!(opts.reserve, opt_flags, MDB_RESERVE);
            set_flags!(opts.append, opt_flags, MDB_APPEND);
            set_flags!(opts.append_duplicate, opt_flags, MDB_APPENDDUP);

            let r = mdb_cursor_put(
                self.cursor.cursor,
                addr_of_mut!(key_data),
                addr_of_mut!(val_data),
                opt_flags,
            );

            match r {
                MDB_MAP_FULL => {
                    return Err(LmdbError::new(
                        r,
                        "The database is full, see mdb_env_set_mapsize()".to_string(),
                    ))
                }
                MDB_TXN_FULL => {
                    return Err(LmdbError::new(
                        r,
                        "the transaction has too many dirty pages".to_string(),
                    ))
                }
                EACCES => {
                    return Err(LmdbError::new(
                        r,
                        "An attempt was made to write in a read-only transaction".to_string(),
                    ))
                }
                EINVAL => return Err(LmdbError::new(r, "Invalid parameter".to_string())),
                x if x != 0 => return Err(LmdbError::new(r, "Unknown error".to_string())),
                _ => {}
            }
            Ok(())
        }
    }
}
