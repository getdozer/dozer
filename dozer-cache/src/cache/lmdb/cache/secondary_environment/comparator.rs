use dozer_storage::errors::StorageError;
use dozer_storage::lmdb::{Database, Transaction};
use dozer_storage::lmdb_sys::{mdb_set_compare, MDB_cmp_func, MDB_val, MDB_SUCCESS};

use crate::cache::index::compare_composite_secondary_index;

pub fn set_sorted_inverted_comparator<T: Transaction>(
    txn: &T,
    db: Database,
    fields: &[usize],
) -> Result<(), StorageError> {
    let comparator: MDB_cmp_func = if fields.len() == 1 {
        None
    } else {
        Some(compare_composite_key)
    };

    if let Some(comparator) = comparator {
        unsafe {
            assert_eq!(
                mdb_set_compare(txn.txn(), db.dbi(), Some(comparator)),
                MDB_SUCCESS
            );
        }
    }
    Ok(())
}

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

#[cfg(test)]
mod tests {
    use std::cmp::Ordering::{self, Equal, Greater, Less};

    use dozer_storage::{
        lmdb::DatabaseFlags, lmdb_sys::mdb_cmp, LmdbEnvironment, RwLmdbEnvironment,
    };

    use dozer_types::json_types::JsonValue;
    use dozer_types::{
        chrono::{DateTime, NaiveDate, TimeZone, Utc},
        ordered_float::OrderedFloat,
        rust_decimal::Decimal,
        types::Field,
    };

    use crate::cache::{index::get_secondary_index, lmdb::utils};

    use super::*;

    fn check_test_cases(mut checker: impl FnMut(&[i64], &[i64], Ordering)) {
        checker(&[1, 1], &[1, 1], Equal);
        checker(&[1, 1], &[1, 2], Less);
        checker(&[1, 1], &[2, 1], Less);
        checker(&[1, 1], &[2, 2], Less);
        checker(&[1, 1], &[1], Greater);
        checker(&[1, 1], &[2], Less);
        checker(&[1, 2], &[1, 1], Greater);
        checker(&[1, 2], &[1, 2], Equal);
        checker(&[1, 2], &[2, 1], Less);
        checker(&[1, 2], &[2, 2], Less);
        checker(&[1, 2], &[1], Greater);
        checker(&[1, 2], &[2], Less);
        checker(&[2, 1], &[1, 1], Greater);
        checker(&[2, 1], &[1, 2], Greater);
        checker(&[2, 1], &[2, 1], Equal);
        checker(&[2, 1], &[2, 2], Less);
        checker(&[2, 1], &[1], Greater);
        checker(&[2, 1], &[2], Greater);
        checker(&[2, 2], &[1, 1], Greater);
        checker(&[2, 2], &[1, 2], Greater);
        checker(&[2, 2], &[2, 1], Greater);
        checker(&[2, 2], &[2, 2], Equal);
        checker(&[2, 2], &[1], Greater);
        checker(&[2, 2], &[2], Greater);
        checker(&[1], &[1, 1], Less);
        checker(&[1], &[1, 2], Less);
        checker(&[1], &[2, 1], Less);
        checker(&[1], &[2, 2], Less);
        checker(&[1], &[1], Equal);
        checker(&[1], &[2], Less);
        checker(&[2], &[1, 1], Greater);
        checker(&[2], &[1, 2], Greater);
        checker(&[2], &[2, 1], Less);
        checker(&[2], &[2, 2], Less);
        checker(&[2], &[1], Greater);
        checker(&[2], &[2], Equal);
    }

    #[test]
    fn test_compare_composite_key() {
        let check = |a: &[i64], b: &[i64], expected: Ordering| {
            let serialize = |a: &[i64]| {
                let a = a.iter().map(|x| Field::Int(*x)).collect::<Vec<_>>();
                let a = a.iter().collect::<Vec<_>>();
                get_secondary_index(&a, false)
            };
            let a = serialize(a);
            let b = serialize(b);
            let a = MDB_val {
                mv_size: a.len() as _,
                mv_data: a.as_ptr() as _,
            };
            let b = MDB_val {
                mv_size: b.len() as _,
                mv_data: b.as_ptr() as _,
            };
            assert_eq!(unsafe { compare_composite_key(&a, &b) }, expected as i32,);
        };

        check_test_cases(check);
    }

    #[test]
    fn test_set_sorted_inverted_comparator() {
        let mut check_single = get_single_key_checker();
        check_single(Some(1), Some(1), Equal);
        check_single(Some(1), Some(2), Less);
        check_single(Some(2), Some(1), Greater);
        check_single(Some(1), None, Less);
        check_single(None, Some(1), Greater);
        check_single(None, None, Equal);

        let check_composite = get_composite_key_checker(2);
        check_test_cases(check_composite);
    }

    fn setup(num_fields: usize) -> (RwLmdbEnvironment, Database) {
        let mut env = utils::create_env(&Default::default()).unwrap().0;
        let db = env
            .create_database(Some("test"), DatabaseFlags::DUP_SORT)
            .unwrap();
        let fields = (0..num_fields).collect::<Vec<_>>();
        let txn = env.begin_txn().unwrap();
        set_sorted_inverted_comparator(&txn, db, &fields).unwrap();
        txn.commit().unwrap();
        (env, db)
    }

    fn get_single_key_checker() -> impl FnMut(Option<i64>, Option<i64>, Ordering) {
        let (env, db) = setup(1);
        move |a: Option<i64>, b: Option<i64>, expected: Ordering| {
            let serialize =
                |a: Option<i64>| get_secondary_index(&[&a.map_or(Field::Null, Field::Int)], true);
            let a = serialize(a);
            let b = serialize(b);
            let a = MDB_val {
                mv_size: a.len() as _,
                mv_data: a.as_ptr() as *mut _,
            };
            let b = MDB_val {
                mv_size: b.len() as _,
                mv_data: b.as_ptr() as *mut _,
            };
            let txn = env.begin_txn().unwrap();
            assert_eq!(
                unsafe { mdb_cmp(txn.txn(), db.dbi(), &a, &b) }.cmp(&0),
                expected
            );
        }
    }

    fn get_composite_key_checker<'a>(
        num_fields: usize,
    ) -> impl FnMut(&[i64], &[i64], Ordering) + 'a {
        let (env, db) = setup(num_fields);
        move |a: &[i64], b: &[i64], expected: Ordering| {
            let serialize = |a: &[i64]| {
                let fields = a.iter().map(|a| Field::Int(*a)).collect::<Vec<_>>();
                let fields = fields.iter().collect::<Vec<_>>();
                get_secondary_index(&fields, false)
            };
            let a = serialize(a);
            let b = serialize(b);
            let a = MDB_val {
                mv_size: a.len() as _,
                mv_data: a.as_ptr() as *mut _,
            };
            let b = MDB_val {
                mv_size: b.len() as _,
                mv_data: b.as_ptr() as *mut _,
            };
            let txn = env.begin_txn().unwrap();
            assert_eq!(
                unsafe { mdb_cmp(txn.txn(), db.dbi(), &a, &b) }.cmp(&0),
                expected
            );
        }
    }

    #[test]
    fn null_is_greater_than_other_thing() {
        let (env, db) = setup(1);
        let txn = env.begin_txn().unwrap();
        let check = |field: &Field| {
            let serialize = |a| get_secondary_index(&[a], true);
            let a = serialize(field);
            let b = serialize(&Field::Null);
            let a = MDB_val {
                mv_size: a.len() as _,
                mv_data: a.as_ptr() as *mut _,
            };
            let b = MDB_val {
                mv_size: b.len() as _,
                mv_data: b.as_ptr() as *mut _,
            };
            assert!(unsafe { mdb_cmp(txn.txn(), db.dbi(), &a, &b) } < 0);
            assert_eq!(field.cmp(&Field::Null), Ordering::Less);
        };

        let test_cases = [
            Field::UInt(u64::MAX),
            Field::Int(i64::MAX),
            Field::Float(OrderedFloat(f64::MAX)),
            Field::Boolean(true),
            Field::String("a".to_string()),
            Field::Text("a".to_string()),
            Field::Binary(vec![255]),
            Field::Decimal(Decimal::new(i64::MAX, 0)),
            Field::Timestamp(DateTime::from(Utc.timestamp_millis_opt(1).unwrap())),
            Field::Date(NaiveDate::from_ymd_opt(2020, 1, 2).unwrap()),
            Field::Json(JsonValue::Array(vec![JsonValue::Number(OrderedFloat(
                255_f64,
            ))])),
        ];
        for a in test_cases.iter() {
            check(a);
        }
    }
}
