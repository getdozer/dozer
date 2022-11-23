use dozer_types::{
    ordered_float::OrderedFloat,
    types::{Field, FieldType, Schema, SortDirection},
};
use lmdb::{Database, Environment, Result, Transaction};
use lmdb_sys::{mdb_set_compare, MDB_cmp_func, MDB_val, MDB_SUCCESS};

use crate::cache::index::compare_composite_secondary_index;

pub fn compared_without_composite_key(field: &Field) -> bool {
    matches!(
        field,
        Field::UInt(_)
            | Field::Int(_)
            | Field::Float(_)
            | Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
    )
}

pub fn set_sorted_inverted_comparator(
    env: &Environment,
    db: Database,
    schema: &Schema,
    fields: &[(usize, SortDirection)],
) -> Result<()> {
    let comparator: MDB_cmp_func = if fields.len() == 1 {
        let (index, direction) = fields[0];
        if direction == SortDirection::Descending {
            match schema.fields[index].typ {
                FieldType::UInt => Some(compare_uint_descending),
                FieldType::Int => Some(compare_int_descending),
                FieldType::Float => Some(compare_float_descending),
                FieldType::Boolean => Some(compare_bool_descending),
                FieldType::String => Some(compare_string_descending),
                FieldType::Text => Some(compare_string_descending),
                FieldType::Binary => Some(compare_binary_descending),
                _ => Some(compare_composite_key),
            }
        } else {
            None
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

macro_rules! comparator {
    ($name:ident, $parser:ident) => {
        unsafe extern "C" fn $name(a: *const MDB_val, b: *const MDB_val) -> std::ffi::c_int {
            let a = $parser(mdb_val_to_slice(&*a));
            let b = $parser(mdb_val_to_slice(&*b));
            a.cmp(&b).reverse() as _
        }
    };
}

fn parse_uint(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().unwrap())
}

comparator!(compare_uint_descending, parse_uint);

fn parse_int(bytes: &[u8]) -> i64 {
    i64::from_be_bytes(bytes.try_into().unwrap())
}

comparator!(compare_int_descending, parse_int);

fn parse_float(bytes: &[u8]) -> OrderedFloat<f64> {
    OrderedFloat(f64::from_be_bytes(bytes.try_into().unwrap()))
}

comparator!(compare_float_descending, parse_float);

fn parse_bool(bytes: &[u8]) -> bool {
    bytes[0] > 0
}

comparator!(compare_bool_descending, parse_bool);

fn parse_string(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).unwrap()
}

comparator!(compare_string_descending, parse_string);

fn parse_binary(bytes: &[u8]) -> &[u8] {
    bytes
}

comparator!(compare_binary_descending, parse_binary);

unsafe fn mdb_val_to_slice(val: &MDB_val) -> &[u8] {
    std::slice::from_raw_parts(val.mv_data as *const u8, val.mv_size)
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
