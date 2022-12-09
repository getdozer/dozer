use dozer_types::types::{Field, SortDirection};
use lmdb::{Database, Environment, Result, Transaction};
use lmdb_sys::{mdb_set_compare, MDB_cmp_func, MDB_val, MDB_SUCCESS};

use crate::cache::index::compare_composite_secondary_index;

pub fn set_sorted_inverted_comparator(
    env: &Environment,
    db: Database,
    fields: &[(usize, SortDirection)],
) -> Result<()> {
    let comparator: MDB_cmp_func = if fields.len() == 1 {
        let (_, direction) = fields[0];
        match direction {
            SortDirection::Descending => Some(compare_single_key_descending),
            SortDirection::Ascending => None,
        }
    } else {
        Some(compare_composite_key)
    };

    if let Some(comparator) = comparator {
        let txn = env.begin_rw_txn()?;
        unsafe {
            assert_eq!(
                mdb_set_compare(txn.txn(), db.dbi(), Some(comparator)),
                MDB_SUCCESS
            );
        }
        txn.commit()
    } else {
        Ok(())
    }
}

unsafe fn mdb_val_to_slice(val: &MDB_val) -> &[u8] {
    std::slice::from_raw_parts(val.mv_data as *const u8, val.mv_size)
}

unsafe extern "C" fn compare_single_key_descending(
    a: *const MDB_val,
    b: *const MDB_val,
) -> std::ffi::c_int {
    let a = Field::from_bytes_borrow(mdb_val_to_slice(&*a))
        .expect("single key comparator should always be able to deserialize field");
    let b = Field::from_bytes_borrow(mdb_val_to_slice(&*b))
        .expect("single key comparator should always be able to deserialize field");
    a.cmp(&b).reverse() as _
}

unsafe extern "C" fn compare_composite_key(
    a: *const MDB_val,
    b: *const MDB_val,
) -> std::ffi::c_int {
    match compare_composite_secondary_index(mdb_val_to_slice(&*a), mdb_val_to_slice(&*b)) {
        Ok(ordering) => ordering as std::ffi::c_int,
        Err(e) => {
            dozer_types::log::error!("Error deserializing secondary index key: {}", e);
            0
        }
    }
}
