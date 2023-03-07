use dozer_storage::lmdb::{Database, Transaction};
use dozer_storage::lmdb_sys as ffi;
use std::{cmp::Ordering, ffi::c_void};

pub fn lmdb_cmp<T: Transaction>(txn: &T, db: Database, a: &[u8], b: &[u8]) -> Ordering {
    let a: ffi::MDB_val = ffi::MDB_val {
        mv_size: a.len(),
        mv_data: a.as_ptr() as *mut c_void,
    };
    let b: ffi::MDB_val = ffi::MDB_val {
        mv_size: b.len(),
        mv_data: b.as_ptr() as *mut c_void,
    };
    let result = unsafe { dozer_storage::lmdb_sys::mdb_cmp(txn.txn(), db.dbi(), &a, &b) };
    result.cmp(&0)
}
