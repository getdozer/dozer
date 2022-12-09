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
    let Ok(a) = Field::from_bytes_borrow(mdb_val_to_slice(&*a)) else {
        dozer_types::log::error!("Error deserializing secondary index field");
        return 0;
    };
    let Ok(b) = Field::from_bytes_borrow(mdb_val_to_slice(&*b)) else {
        dozer_types::log::error!("Error deserializing secondary index field");
        return 0;
    };
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

#[cfg(test)]
mod tests {
    use std::cmp::Ordering::{self, Equal, Greater, Less};

    use dozer_types::{
        bincode,
        chrono::{DateTime, NaiveDate, TimeZone, Utc},
        ordered_float::OrderedFloat,
        rust_decimal::Decimal,
    };
    use lmdb_sys::mdb_cmp;

    use crate::cache::{index::get_secondary_index, lmdb::utils, CacheOptions};

    use super::*;

    #[test]
    fn test_compare_single_key_descending() {
        let check = |field1: Field, field2: Field, expected: Ordering| {
            let a = field1.to_bytes();
            let b = field2.to_bytes();
            let a = MDB_val {
                mv_size: a.len() as _,
                mv_data: a.as_ptr() as _,
            };
            let b = MDB_val {
                mv_size: b.len() as _,
                mv_data: b.as_ptr() as _,
            };
            assert_eq!(
                unsafe { compare_single_key_descending(&a, &b) },
                expected as i32
            );
        };

        check(Field::Int(1), Field::Int(2), Greater);
        check(Field::Int(1), Field::Int(1), Equal);
        check(Field::Int(1), Field::Int(0), Less);
        check(Field::UInt(1), Field::UInt(2), Greater);
        check(Field::UInt(1), Field::UInt(1), Equal);
        check(Field::UInt(1), Field::UInt(0), Less);
        check(
            Field::Float(OrderedFloat(1.0)),
            Field::Float(OrderedFloat(2.0)),
            Greater,
        );
        check(
            Field::Float(OrderedFloat(1.0)),
            Field::Float(OrderedFloat(1.0)),
            Equal,
        );
        check(
            Field::Float(OrderedFloat(1.0)),
            Field::Float(OrderedFloat(0.0)),
            Less,
        );
        check(Field::Boolean(false), Field::Boolean(true), Greater);
        check(Field::Boolean(false), Field::Boolean(false), Equal);
        check(Field::Boolean(true), Field::Boolean(true), Equal);
        check(Field::Boolean(true), Field::Boolean(false), Less);
        check(
            Field::String("b".to_string()),
            Field::String("c".to_string()),
            Greater,
        );
        check(
            Field::String("b".to_string()),
            Field::String("b".to_string()),
            Equal,
        );
        check(
            Field::String("b".to_string()),
            Field::String("a".to_string()),
            Less,
        );
        check(
            Field::Text("b".to_string()),
            Field::Text("c".to_string()),
            Greater,
        );
        check(
            Field::Text("b".to_string()),
            Field::Text("b".to_string()),
            Equal,
        );
        check(
            Field::Text("b".to_string()),
            Field::Text("a".to_string()),
            Less,
        );
        check(Field::Binary(vec![1]), Field::Binary(vec![2]), Greater);
        check(Field::Binary(vec![1]), Field::Binary(vec![1]), Equal);
        check(Field::Binary(vec![1]), Field::Binary(vec![0]), Less);
        check(
            Field::Decimal(Decimal::new(1, 1)),
            Field::Decimal(Decimal::new(2, 1)),
            Greater,
        );
        check(
            Field::Decimal(Decimal::new(1, 1)),
            Field::Decimal(Decimal::new(1, 1)),
            Equal,
        );
        check(
            Field::Decimal(Decimal::new(1, 1)),
            Field::Decimal(Decimal::new(0, 1)),
            Less,
        );
        check(
            Field::Timestamp(DateTime::from(Utc.timestamp_millis(1))),
            Field::Timestamp(DateTime::from(Utc.timestamp_millis(2))),
            Greater,
        );
        check(
            Field::Timestamp(DateTime::from(Utc.timestamp_millis(1))),
            Field::Timestamp(DateTime::from(Utc.timestamp_millis(1))),
            Equal,
        );
        check(
            Field::Timestamp(DateTime::from(Utc.timestamp_millis(1))),
            Field::Timestamp(DateTime::from(Utc.timestamp_millis(0))),
            Less,
        );
        check(
            Field::Date(NaiveDate::from_ymd(2020, 1, 2)),
            Field::Date(NaiveDate::from_ymd(2020, 1, 3)),
            Greater,
        );
        check(
            Field::Date(NaiveDate::from_ymd(2020, 1, 2)),
            Field::Date(NaiveDate::from_ymd(2020, 1, 2)),
            Equal,
        );
        check(
            Field::Date(NaiveDate::from_ymd(2020, 1, 2)),
            Field::Date(NaiveDate::from_ymd(2020, 1, 1)),
            Less,
        );
        check(Field::Bson(vec![1]), Field::Bson(vec![2]), Greater);
        check(Field::Bson(vec![1]), Field::Bson(vec![1]), Equal);
        check(Field::Bson(vec![1]), Field::Bson(vec![0]), Less);
        check(Field::Null, Field::Null, Equal);
    }

    fn check_desc_asc_cases(checker: impl Fn(&[i64], &[i64], Ordering)) {
        checker(&[1, 1], &[1, 1], Equal);
        checker(&[1, 1], &[1, 2], Less);
        checker(&[1, 1], &[2, 1], Greater);
        checker(&[1, 1], &[2, 2], Greater);
        checker(&[1, 1], &[1], Greater);
        checker(&[1, 1], &[2], Greater);
        checker(&[1, 2], &[1, 1], Greater);
        checker(&[1, 2], &[1, 2], Equal);
        checker(&[1, 2], &[2, 1], Greater);
        checker(&[1, 2], &[2, 2], Greater);
        checker(&[1, 2], &[1], Greater);
        checker(&[1, 2], &[2], Greater);
        checker(&[2, 1], &[1, 1], Less);
        checker(&[2, 1], &[1, 2], Less);
        checker(&[2, 1], &[2, 1], Equal);
        checker(&[2, 1], &[2, 2], Less);
        checker(&[2, 1], &[1], Less);
        checker(&[2, 1], &[2], Greater);
        checker(&[2, 2], &[1, 1], Less);
        checker(&[2, 2], &[1, 2], Less);
        checker(&[2, 2], &[2, 1], Greater);
        checker(&[2, 2], &[2, 2], Equal);
        checker(&[2, 2], &[1], Less);
        checker(&[2, 2], &[2], Greater);
        checker(&[1], &[1, 1], Less);
        checker(&[1], &[1, 2], Less);
        checker(&[1], &[2, 1], Greater);
        checker(&[1], &[2, 2], Greater);
        checker(&[1], &[1], Equal);
        checker(&[1], &[2], Greater);
        checker(&[2], &[1, 1], Less);
        checker(&[2], &[1, 2], Less);
        checker(&[2], &[2, 1], Less);
        checker(&[2], &[2, 2], Less);
        checker(&[2], &[1], Less);
        checker(&[2], &[2], Equal);
    }

    #[test]
    fn test_compare_composite_key() {
        let check = |directions: &[SortDirection], a: &[i64], b: &[i64], expected: Ordering| {
            let serialize = |a: &[i64]| {
                let a = a
                    .iter()
                    .map(|x| Field::Int(*x))
                    .zip(directions.iter().copied())
                    .collect::<Vec<_>>();
                bincode::serialize(&a).unwrap()
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

        let check_asc_asc = |a: &[i64], b: &[i64], expected: Ordering| {
            check(
                &[SortDirection::Ascending, SortDirection::Ascending],
                a,
                b,
                expected,
            )
        };
        check_asc_asc(&[1, 1], &[1, 1], Equal);
        check_asc_asc(&[1, 1], &[1, 2], Less);
        check_asc_asc(&[1, 1], &[2, 1], Less);
        check_asc_asc(&[1, 1], &[2, 2], Less);
        check_asc_asc(&[1, 1], &[1], Greater);
        check_asc_asc(&[1, 1], &[2], Less);
        check_asc_asc(&[1, 2], &[1, 1], Greater);
        check_asc_asc(&[1, 2], &[1, 2], Equal);
        check_asc_asc(&[1, 2], &[2, 1], Less);
        check_asc_asc(&[1, 2], &[2, 2], Less);
        check_asc_asc(&[1, 2], &[1], Greater);
        check_asc_asc(&[1, 2], &[2], Less);
        check_asc_asc(&[2, 1], &[1, 1], Greater);
        check_asc_asc(&[2, 1], &[1, 2], Greater);
        check_asc_asc(&[2, 1], &[2, 1], Equal);
        check_asc_asc(&[2, 1], &[2, 2], Less);
        check_asc_asc(&[2, 1], &[1], Greater);
        check_asc_asc(&[2, 1], &[2], Greater);
        check_asc_asc(&[2, 2], &[1, 1], Greater);
        check_asc_asc(&[2, 2], &[1, 2], Greater);
        check_asc_asc(&[2, 2], &[2, 1], Greater);
        check_asc_asc(&[2, 2], &[2, 2], Equal);
        check_asc_asc(&[2, 2], &[1], Greater);
        check_asc_asc(&[2, 2], &[2], Greater);
        check_asc_asc(&[1], &[1, 1], Less);
        check_asc_asc(&[1], &[1, 2], Less);
        check_asc_asc(&[1], &[2, 1], Less);
        check_asc_asc(&[1], &[2, 2], Less);
        check_asc_asc(&[1], &[1], Equal);
        check_asc_asc(&[1], &[2], Less);
        check_asc_asc(&[2], &[1, 1], Greater);
        check_asc_asc(&[2], &[1, 2], Greater);
        check_asc_asc(&[2], &[2, 1], Less);
        check_asc_asc(&[2], &[2, 2], Less);
        check_asc_asc(&[2], &[1], Greater);
        check_asc_asc(&[2], &[2], Equal);

        let check_asc_desc = |a: &[i64], b: &[i64], expected: Ordering| {
            check(
                &[SortDirection::Ascending, SortDirection::Descending],
                a,
                b,
                expected,
            )
        };
        check_asc_desc(&[1, 1], &[1, 1], Equal);
        check_asc_desc(&[1, 1], &[1, 2], Greater);
        check_asc_desc(&[1, 1], &[2, 1], Less);
        check_asc_desc(&[1, 1], &[2, 2], Less);
        check_asc_desc(&[1, 1], &[1], Greater);
        check_asc_desc(&[1, 1], &[2], Less);
        check_asc_desc(&[1, 2], &[1, 1], Less);
        check_asc_desc(&[1, 2], &[1, 2], Equal);
        check_asc_desc(&[1, 2], &[2, 1], Less);
        check_asc_desc(&[1, 2], &[2, 2], Less);
        check_asc_desc(&[1, 2], &[1], Greater);
        check_asc_desc(&[1, 2], &[2], Less);
        check_asc_desc(&[2, 1], &[1, 1], Greater);
        check_asc_desc(&[2, 1], &[1, 2], Greater);
        check_asc_desc(&[2, 1], &[2, 1], Equal);
        check_asc_desc(&[2, 1], &[2, 2], Greater);
        check_asc_desc(&[2, 1], &[1], Greater);
        check_asc_desc(&[2, 1], &[2], Greater);
        check_asc_desc(&[2, 2], &[1, 1], Greater);
        check_asc_desc(&[2, 2], &[1, 2], Greater);
        check_asc_desc(&[2, 2], &[2, 1], Less);
        check_asc_desc(&[2, 2], &[2, 2], Equal);
        check_asc_desc(&[2, 2], &[1], Greater);
        check_asc_desc(&[2, 2], &[2], Greater);
        check_asc_desc(&[1], &[1, 1], Less);
        check_asc_desc(&[1], &[1, 2], Less);
        check_asc_desc(&[1], &[2, 1], Less);
        check_asc_desc(&[1], &[2, 2], Less);
        check_asc_desc(&[1], &[1], Equal);
        check_asc_desc(&[1], &[2], Less);
        check_asc_desc(&[2], &[1, 1], Greater);
        check_asc_desc(&[2], &[1, 2], Greater);
        check_asc_desc(&[2], &[2, 1], Less);
        check_asc_desc(&[2], &[2, 2], Less);
        check_asc_desc(&[2], &[1], Greater);
        check_asc_desc(&[2], &[2], Equal);

        let check_desc_asc = |a: &[i64], b: &[i64], expected: Ordering| {
            check(
                &[SortDirection::Descending, SortDirection::Ascending],
                a,
                b,
                expected,
            )
        };
        check_desc_asc_cases(check_desc_asc);

        let check_desc_desc = |a: &[i64], b: &[i64], expected: Ordering| {
            check(
                &[SortDirection::Descending, SortDirection::Descending],
                a,
                b,
                expected,
            )
        };
        check_desc_desc(&[1, 1], &[1, 1], Equal);
        check_desc_desc(&[1, 1], &[1, 2], Greater);
        check_desc_desc(&[1, 1], &[2, 1], Greater);
        check_desc_desc(&[1, 1], &[2, 2], Greater);
        check_desc_desc(&[1, 1], &[1], Greater);
        check_desc_desc(&[1, 1], &[2], Greater);
        check_desc_desc(&[1, 2], &[1, 1], Less);
        check_desc_desc(&[1, 2], &[1, 2], Equal);
        check_desc_desc(&[1, 2], &[2, 1], Greater);
        check_desc_desc(&[1, 2], &[2, 2], Greater);
        check_desc_desc(&[1, 2], &[1], Greater);
        check_desc_desc(&[1, 2], &[2], Greater);
        check_desc_desc(&[2, 1], &[1, 1], Less);
        check_desc_desc(&[2, 1], &[1, 2], Less);
        check_desc_desc(&[2, 1], &[2, 1], Equal);
        check_desc_desc(&[2, 1], &[2, 2], Greater);
        check_desc_desc(&[2, 1], &[1], Less);
        check_desc_desc(&[2, 1], &[2], Greater);
        check_desc_desc(&[2, 2], &[1, 1], Less);
        check_desc_desc(&[2, 2], &[1, 2], Less);
        check_desc_desc(&[2, 2], &[2, 1], Less);
        check_desc_desc(&[2, 2], &[2, 2], Equal);
        check_desc_desc(&[2, 2], &[1], Less);
        check_desc_desc(&[2, 2], &[2], Greater);
        check_desc_desc(&[1], &[1, 1], Less);
        check_desc_desc(&[1], &[1, 2], Less);
        check_desc_desc(&[1], &[2, 1], Greater);
        check_desc_desc(&[1], &[2, 2], Greater);
        check_desc_desc(&[1], &[1], Equal);
        check_desc_desc(&[1], &[2], Greater);
        check_desc_desc(&[2], &[1, 1], Less);
        check_desc_desc(&[2], &[1, 2], Less);
        check_desc_desc(&[2], &[2, 1], Less);
        check_desc_desc(&[2], &[2, 2], Less);
        check_desc_desc(&[2], &[1], Less);
        check_desc_desc(&[2], &[2], Equal);
    }

    #[test]
    fn test_set_sorted_inverted_comparator() {
        let check_single_asc = get_single_key_checker(SortDirection::Ascending);
        check_single_asc(1, 1, Equal);
        check_single_asc(1, 2, Less);
        check_single_asc(2, 1, Greater);

        let check_single_desc = get_single_key_checker(SortDirection::Descending);
        check_single_desc(1, 1, Equal);
        check_single_desc(1, 2, Greater);
        check_single_desc(2, 1, Less);

        let check_composite =
            get_composite_key_checker(&[SortDirection::Descending, SortDirection::Ascending]);
        check_desc_asc_cases(check_composite);
    }

    fn setup(directions: &[SortDirection]) -> (Environment, Database) {
        let options = CacheOptions::default();
        let env = utils::init_env(&options).unwrap();
        let db = utils::init_db(&env, Some("test"), &options, true, false).unwrap();
        let fields = directions.iter().copied().enumerate().collect::<Vec<_>>();
        set_sorted_inverted_comparator(&env, db, &fields).unwrap();
        (env, db)
    }

    fn get_single_key_checker(direction: SortDirection) -> impl Fn(i64, i64, Ordering) {
        let (env, db) = setup(&[direction]);
        move |a: i64, b: i64, expected: Ordering| {
            let serialize = |a| get_secondary_index(&[(&Field::Int(a), direction)], true).unwrap();
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
            let txn = env.begin_ro_txn().unwrap();
            assert_eq!(
                unsafe { mdb_cmp(txn.txn(), db.dbi(), &a, &b) },
                expected as i32
            );
        }
    }

    fn get_composite_key_checker<'a>(
        directions: &'a [SortDirection],
    ) -> impl Fn(&[i64], &[i64], Ordering) + 'a {
        let (env, db) = setup(directions);
        move |a: &[i64], b: &[i64], expected: Ordering| {
            let serialize = |a: &[i64]| {
                let fields = a.iter().map(|a| Field::Int(*a)).collect::<Vec<_>>();
                let fields = fields
                    .iter()
                    .zip(directions.iter().copied())
                    .collect::<Vec<_>>();
                get_secondary_index(&fields, false).unwrap()
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
            let txn = env.begin_ro_txn().unwrap();
            assert_eq!(
                unsafe { mdb_cmp(txn.txn(), db.dbi(), &a, &b) },
                expected as i32
            );
        }
    }
}
