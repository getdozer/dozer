use std::{ptr, slice};
use std::any::TypeId;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ptr::addr_of_mut;
use std::sync::{Arc, RwLock};
use libc::{ENOENT, EACCES, EAGAIN, ENOMEM, EINVAL, ENOSPC, EIO, mode_t, size_t, c_uint, c_void, c_int};
use unixstring::UnixString;
use lmdb_sys::{MDB_env, mdb_env_create, MDB_VERSION_MISMATCH, MDB_INVALID, mdb_env_open, mdb_env_set_mapsize, MDB_txn, mdb_txn_begin, MDB_RDONLY, MDB_PANIC, MDB_MAP_RESIZED, MDB_READERS_FULL, MDB_dbi, mdb_dbi_open, MDB_CREATE, MDB_DUPSORT, MDB_INTEGERKEY, MDB_DUPFIXED, MDB_NOTFOUND, MDB_DBS_FULL, mdb_put, MDB_val, MDB_NODUPDATA, MDB_NOOVERWRITE, MDB_MAP_FULL, MDB_TXN_FULL, mdb_get, mdb_env_set_maxdbs, mdb_dbi_close, mdb_txn_commit, mdb_txn_abort, mdb_del, mdb_env_close, MDB_NOSYNC, MDB_NOMETASYNC, MDB_NOSUBDIR, MDB_WRITEMAP};


#[derive(Debug, Clone)]
pub struct LmdbError {
    err_no: i32,
    err_str: String
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

/***********************************************************************************
  Environment
 ***********************************************************************************/

pub struct Environment {
    env_ptr: *mut MDB_env
}

unsafe impl Send for Environment {}
unsafe impl Sync for Environment {}

#[derive(Debug, Copy, Clone)]
pub struct EnvOptions {
    pub map_size: Option<size_t>,
    pub max_dbs: Option<u32>,
    pub no_sync: bool,
    pub no_meta_sync: bool,
    pub no_subdir: bool,
    pub writable_mem_map: bool
}

impl EnvOptions {
    pub fn default() -> Self {
        Self {
            map_size: None, max_dbs: None, no_sync: false, no_meta_sync: false,
            no_subdir: false, writable_mem_map: false
        }
    }
}

impl Environment {

    pub fn new(path: String, opts: Option<EnvOptions>) -> Result<Environment, LmdbError> {

        unsafe {

            let mut env_ptr: *mut MDB_env = ptr::null_mut();

            let mut r = mdb_env_create(addr_of_mut!(env_ptr));
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

            if !opts.is_none() {
                if opts.unwrap().map_size.is_some() {
                    if mdb_env_set_mapsize(env_ptr, opts.unwrap().map_size.unwrap()) != 0 {
                        return Err(LmdbError::new(r, "Invalid map size specified".to_string()));
                    }
                }
                if opts.unwrap().max_dbs.is_some() {
                    if mdb_env_set_maxdbs(env_ptr, opts.unwrap().max_dbs.unwrap()) != 0 {
                        return Err(LmdbError::new(r, "Invalid map size specified".to_string()));
                    }
                }
                if opts.unwrap().no_sync { flags |= MDB_NOSYNC; }
                if opts.unwrap().no_meta_sync { flags |= MDB_NOMETASYNC; }
                if opts.unwrap().no_subdir { flags |= MDB_NOSUBDIR; }
                if opts.unwrap().writable_mem_map { flags |= MDB_WRITEMAP; }
            }

            let r = mdb_env_open(
                env_ptr,
                UnixString::from_string(path).unwrap().as_ptr(),
                flags, 0o664
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

            Ok(Environment { env_ptr })
        }
    }

}

impl Drop for Environment {
    fn drop(&mut self) {
        unsafe {
            mdb_env_close(self.env_ptr);
        }
    }
}

/***********************************************************************************
  Transaction
 ***********************************************************************************/


pub struct Transaction {
    env: Arc<Environment>,
    txn: *mut MDB_txn,
    parent: Option<Arc<Transaction>>
}

unsafe impl Send for Transaction {}
unsafe impl Sync for Transaction {}


impl Transaction {


    pub fn begin(env: Arc<Environment>) -> Result<Transaction, LmdbError> {

        unsafe {

            let mut txn_ptr: *mut MDB_txn = ptr::null_mut();

            let r = mdb_txn_begin(env.env_ptr,  ptr::null_mut(), 0, addr_of_mut!(txn_ptr));
            match r {
                MDB_PANIC => { return Err(LmdbError::new(r, "A fatal error occurred earlier and the environment must be shut down".to_string())) }
                MDB_MAP_RESIZED => { return Err(LmdbError::new(r, "Another process wrote data beyond this MDB_env's mapsize and this environment's map must be resized as well. See mdb_env_set_mapsize()".to_string())) }
                MDB_READERS_FULL => { return Err(LmdbError::new(r, "A read-only transaction was requested and the reader lock table is full. See mdb_env_set_maxreaders()".to_string())) }
                ENOMEM => { return Err(LmdbError::new(r, "Out of Memory".to_string())) }
                x if x != 0 => { return Err(LmdbError::new(r, "Unknown error".to_string())) }
                _ => {}
            }

            Ok(Transaction {env, txn: txn_ptr, parent: None})

        }

    }

    pub fn child(env: Arc<Environment>, parent: Arc<Transaction>) -> Result<Transaction, LmdbError> {

        unsafe {

            let mut txn_ptr: *mut MDB_txn = ptr::null_mut();

            let r = mdb_txn_begin(env.env_ptr,  parent.txn, 0, addr_of_mut!(txn_ptr));
            match r {
                MDB_PANIC => { return Err(LmdbError::new(r, "A fatal error occurred earlier and the environment must be shut down".to_string())) }
                MDB_MAP_RESIZED => { return Err(LmdbError::new(r, "Another process wrote data beyond this MDB_env's mapsize and this environment's map must be resized as well. See mdb_env_set_mapsize()".to_string())) }
                MDB_READERS_FULL => { return Err(LmdbError::new(r, "A read-only transaction was requested and the reader lock table is full. See mdb_env_set_maxreaders()".to_string())) }
                ENOMEM => { return Err(LmdbError::new(r, "Out of Memory".to_string())) }
                x if x != 0 => { return Err(LmdbError::new(r, "Unknown error".to_string())) }
                _ => {}
            }

            Ok(Transaction {env, txn: txn_ptr, parent: Some(parent) })

        }
    }

    pub fn commit(&self) -> Result<(), LmdbError> {
        unsafe {
            let r = mdb_txn_commit(self.txn);
            match r {
                EINVAL => { return Err(LmdbError::new(r, "An invalid parameter was specified".to_string())) }
                ENOSPC => { return Err(LmdbError::new(r, "No more space on disk".to_string())) }
                EIO => { return Err(LmdbError::new(r, "S low-level I/O error occurred while writing".to_string())) }
                ENOMEM => { return Err(LmdbError::new(r, "Out of memory".to_string())) }
                x if x != 0 => { return Err(LmdbError::new(r, "Unknown error".to_string())) }
                _ => {}
            }
            Ok(())
        }
    }

    pub fn abort(&self) -> Result<(), LmdbError> {
        unsafe {
            mdb_txn_abort(self.txn);
            Ok(())
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {

    }
}



#[derive(Debug, Copy, Clone)]
pub struct DatabaseOptions {
    pub create: bool,
    pub allow_duplicate_keys: bool,
    pub integer_keys: bool,
    pub fixed_key_size: bool
}

impl DatabaseOptions {

    pub fn new(create: bool, allow_duplicate_keys: bool, integer_keys: bool, fixed_key_size: bool) -> Self {
        Self { create, allow_duplicate_keys, integer_keys, fixed_key_size }
    }

    pub fn default() -> DatabaseOptions {
        return DatabaseOptions {
            create: true, allow_duplicate_keys: false, integer_keys: false, fixed_key_size: false
        }
    }
}

/***********************************************************************************
  Database
 ***********************************************************************************/


pub struct Database {
    env: Arc<Environment>,
    dbi : MDB_dbi
}

unsafe impl Send for Database {}
unsafe impl Sync for Database {}

#[derive(Debug, Copy, Clone)]
pub struct PutOptions {
    no_duplicates: bool,
    no_overwrite: bool
}


impl Database {

    pub fn open(env: Arc<Environment>, txn: &Transaction, name: String, opts: Option<DatabaseOptions>) -> Result<Database, LmdbError> {

        unsafe {

            let mut dbi: MDB_dbi = 0;

            let mut opt_flags: c_uint = 0;
            if opts.is_some() {
                if opts.unwrap().create { opt_flags |= MDB_CREATE }
                if opts.unwrap().allow_duplicate_keys { opt_flags |= MDB_DUPSORT }
                if opts.unwrap().integer_keys { opt_flags |= MDB_INTEGERKEY }
                if opts.unwrap().fixed_key_size { opt_flags |= MDB_DUPFIXED }
            }

            let r = mdb_dbi_open(
                txn.txn,
                UnixString::from_string(name).unwrap().as_ptr(),
                opt_flags, addr_of_mut!(dbi)
            );

            match r {
                MDB_NOTFOUND => { return Err(LmdbError::new(r, "The specified database doesn't exist in the environment and MDB_CREATE was not specified".to_string())) }
                MDB_DBS_FULL => { return Err(LmdbError::new(r, "Too many databases have been opened. See mdb_env_set_maxdbs()".to_string())) }
                x if x != 0 => { return Err(LmdbError::new(r, "Unknown error".to_string())) }
                _ => {}
            }

            Ok(Database {env, dbi})
        }

    }


    pub fn put(&self, txn: &Transaction, key: &[u8], value: &[u8], opts: Option<PutOptions>) -> Result<(), LmdbError> {

        unsafe {


            let mut key_data = MDB_val { mv_size: key.len(), mv_data: key.as_ptr() as *mut c_void};
            let mut val_data = MDB_val { mv_size: value.len(), mv_data: value.as_ptr() as *mut c_void};

            let mut opt_flags: c_uint = 0;
            if opts.is_some() {
                if opts.unwrap().no_duplicates { opt_flags |= MDB_NODUPDATA }
                if opts.unwrap().no_overwrite { opt_flags |= MDB_NOOVERWRITE }
            }

            let r = mdb_put(txn.txn, self.dbi, addr_of_mut!(key_data), addr_of_mut!(val_data), opt_flags);
            match r {
                MDB_MAP_FULL => { return Err(LmdbError::new(r, "The database is full, see mdb_env_set_mapsize()".to_string())) }
                MDB_TXN_FULL => { return Err(LmdbError::new(r, "the transaction has too many dirty pages".to_string())) }
                EACCES => { return Err(LmdbError::new(r, "An attempt was made to write in a read-only transaction".to_string())) }
                EINVAL => { return Err(LmdbError::new(r, "Invalid parameter".to_string())) }
                x if x != 0 => { return Err(LmdbError::new(r, "Unknown error".to_string())) }
                _ => {}
            }

            Ok(())
        }

    }

    pub fn get(&self, txn: &Transaction, mut key: &[u8]) -> Result<Option<&[u8]>, LmdbError> {

        unsafe {

            let mut key_data = MDB_val { mv_size: key.len(), mv_data: key.as_ptr() as *mut c_void };
            let mut val_data = MDB_val {mv_size: 0, mv_data: ptr::null_mut()};

            let r = mdb_get(txn.txn, self.dbi, addr_of_mut!(key_data), addr_of_mut!(val_data));
            match r {
                MDB_NOTFOUND => { return Ok(None) }
                EINVAL => { return Err(LmdbError::new(r, "Invalid parameter".to_string())) }
                x if x != 0 => { return Err(LmdbError::new(r, "Unknown error".to_string())) }
                _ => {}
            }

            Ok(Some(slice::from_raw_parts(val_data.mv_data as *mut u8, val_data.mv_size as usize)))
        }

    }

    pub fn del(&self, txn: &Transaction, mut key: &[u8], mut value: Option<&[u8]>) -> Result<bool, LmdbError> {

        unsafe {

            let mut key_data = MDB_val { mv_size: key.len(), mv_data: key.as_ptr() as *mut c_void };
            let mut val_data = if value.is_some() { Some(MDB_val {mv_size: value.unwrap().len(), mv_data: value.unwrap().as_ptr() as *mut c_void}) } else { None };

            let mut r : c_int = -1;
            if val_data.is_some() {
                let mut val_data_unwrapped = val_data.unwrap();
                r = mdb_del(txn.txn, self.dbi, addr_of_mut!(key_data), addr_of_mut!(val_data_unwrapped));
            }
            else {
                r = mdb_del(txn.txn, self.dbi, addr_of_mut!(key_data), ptr::null_mut());
            }

            match r {
                MDB_NOTFOUND => { return Ok(false) }
                EACCES => { return Err(LmdbError::new(r, "An attempt was made to write in a read-only transaction".to_string())) }
                EINVAL => { return Err(LmdbError::new(r, "Invalid parameter".to_string())) }
                x if x != 0 => { return Err(LmdbError::new(r, "Unknown error".to_string())) }
                _ => {}
            }

            Ok(true)
        }

    }

}

impl Drop for Database {
    fn drop(&mut self) {

    }
}
