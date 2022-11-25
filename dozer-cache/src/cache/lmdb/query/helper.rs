use crate::errors::{CacheError, QueryError};
use dozer_types::{bincode, serde};
use lmdb::{Database, RoTransaction, Transaction};
use lmdb_sys as ffi;
use std::ffi::c_void;
pub fn get<T>(txn: &RoTransaction, db: Database, id: &[u8]) -> Result<T, CacheError>
where
    T: for<'a> serde::de::Deserialize<'a>,
{
    let rec = txn
        .get(db, &id)
        .map_err(|e| CacheError::QueryError(QueryError::GetValue(e)))?;
    bincode::deserialize(rec).map_err(CacheError::map_deserialization_error)
}

pub fn lmdb_cmp(txn: &RoTransaction, db: &Database, a: &[u8], b: Option<&Vec<u8>>) -> i32 {
    if let Some(b) = b {
        let key_val: ffi::MDB_val = ffi::MDB_val {
            mv_size: a.len(),
            mv_data: a.as_ptr() as *mut c_void,
        };
        let start_key_val: ffi::MDB_val = ffi::MDB_val {
            mv_size: b.len(),
            mv_data: b.as_ptr() as *mut c_void,
        };
        unsafe { lmdb_sys::mdb_cmp(txn.txn(), db.dbi(), &key_val, &start_key_val) }
    } else {
        2
    }
}

pub fn lmdb_stat<T: Transaction>(txn: &T, db: Database) -> Result<ffi::MDB_stat, lmdb::Error> {
    let mut stat = ffi::MDB_stat {
        ms_psize: 0,
        ms_depth: 0,
        ms_branch_pages: 0,
        ms_leaf_pages: 0,
        ms_overflow_pages: 0,
        ms_entries: 0,
    };
    let code = unsafe { lmdb_sys::mdb_stat(txn.txn(), db.dbi(), &mut stat) };
    if code == ffi::MDB_SUCCESS {
        Ok(stat)
    } else {
        Err(lmdb::Error::from_err_code(code))
    }
}
