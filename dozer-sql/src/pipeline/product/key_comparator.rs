use std::cmp::Ordering;

use dozer_core::dag::errors::ExecutionError;
use lmdb_sys::MDB_val;

pub unsafe extern "C" fn compare_join_keys(
    a: *const MDB_val,
    b: *const MDB_val,
) -> std::ffi::c_int {
    match compare_join_index(mdb_val_to_slice(&*a), mdb_val_to_slice(&*b)) {
        Ok(ordering) => ordering as std::ffi::c_int,
        Err(e) => {
            dozer_types::log::error!("Error deserializing Join index key: {}", e);
            0
        }
    }
}

unsafe fn mdb_val_to_slice(val: &MDB_val) -> &[u8] {
    std::slice::from_raw_parts(val.mv_data as *const u8, val.mv_size)
}

pub fn compare_join_index(a: &[u8], b: &[u8]) -> Result<Ordering, ExecutionError> {
    let a_length = u16::from_be_bytes(a[0..2].try_into().unwrap()) as usize;
    let b_length = u16::from_be_bytes(b[0..2].try_into().unwrap()) as usize;
    //let shorter = cmp::min(a_length, b_length);

    for (ai, bi) in a[2..a_length].iter().zip(b[2..b_length].iter()) {
        match ai.cmp(bi) {
            Ordering::Equal => continue,
            ord => return Ok(ord),
        }
    }

    match a_length.cmp(&b_length) {
        Ordering::Less => Ok(Ordering::Less),
        Ordering::Equal => {
            if a.len() == a_length + 2 || b.len() == b_length + 2 {
                Ok(Ordering::Equal)
            } else {
                Ok(compare_record_key(&a[a_length..], &b[b_length..]))
            }
        }
        Ordering::Greater => Ok(Ordering::Greater),
    }
}

fn compare_record_key(a: &[u8], b: &[u8]) -> Ordering {
    for (ai, bi) in a.iter().zip(b.iter()) {
        match ai.cmp(bi) {
            Ordering::Equal => continue,
            ord => return ord,
        }
    }
    a.len().cmp(&b.len())
}
