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

#[test]
fn index_comparator_cmp_test() {
    let a = get_full_key(String::from("Alice"), 10);
    let b = get_full_key(String::from("Bob"), 10);

    let order = compare_join_index(a.as_slice(), b.as_slice())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(order, Ordering::Less);

    let a = get_full_key(String::from("Alice"), 20);
    let b = get_full_key(String::from("Bob"), 10);

    let order = compare_join_index(a.as_slice(), b.as_slice())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(order, Ordering::Less);

    let a = get_full_key(String::from("Bob"), 10);
    let b = get_full_key(String::from("Alice"), 10);

    let order = compare_join_index(a.as_slice(), b.as_slice())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(order, Ordering::Greater);

    let a = get_full_key(String::from("Bob"), 10);
    let b = get_full_key(String::from("Alice"), 20);

    let order = compare_join_index(a.as_slice(), b.as_slice())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(order, Ordering::Greater);

    let a = get_full_key(String::from("Alice"), 10);
    let b = get_full_key(String::from("Alice"), 10);

    let order = compare_join_index(a.as_slice(), b.as_slice())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(order, Ordering::Equal);

    let a = get_full_key(String::from("Alice"), 10);
    let b = get_full_key(String::from("Alice"), 20);

    let order = compare_join_index(a.as_slice(), b.as_slice())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(order, Ordering::Less);

    let a = get_full_key(String::from("Alice"), 20);
    let b = get_full_key(String::from("Alice"), 10);

    let order = compare_join_index(a.as_slice(), b.as_slice())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(order, Ordering::Greater);
}

#[test]
fn index_comparator_join_test() {
    let a = get_join_key(String::from("Bob"));
    let b = get_full_key(String::from("Bob"), 10);

    let order = compare_join_index(a.as_slice(), b.as_slice())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(order, Ordering::Equal);

    let a = get_full_key(String::from("Bob"), 10);
    let b = get_join_key(String::from("Bob"));

    let order = compare_join_index(a.as_slice(), b.as_slice())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(order, Ordering::Equal);

    let a = get_join_key(String::from("Alice"));
    let b = get_full_key(String::from("Bob"), 10);

    let order = compare_join_index(a.as_slice(), b.as_slice())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(order, Ordering::Less);

    let a = get_join_key(String::from("Bob"));
    let b = get_full_key(String::from("Alice"), 10);

    let order = compare_join_index(a.as_slice(), b.as_slice())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(order, Ordering::Greater);
}

#[test]
fn index_comparator_lookup_less_test() {
    let a = get_full_key(String::from("Bob"), 5);
    let b = get_full_key(String::from("Bob"), 10);

    let order = compare_join_index(a.as_slice(), b.as_slice())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(order, Ordering::Less);

    let a = get_full_key(String::from("Bob"), 10);
    let b = get_full_key(String::from("Bob"), 5);

    let order = compare_join_index(a.as_slice(), b.as_slice())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(order, Ordering::Greater);

    let a = get_full_key(String::from("Bob"), 10);
    let b = get_full_key(String::from("Bob"), 10);

    let order = compare_join_index(a.as_slice(), b.as_slice())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(order, Ordering::Equal);
}

fn get_full_key(from: String, arg: u64) -> Vec<u8> {
    let mut a_string = from.as_bytes().to_vec();
    let mut a_len = (a_string.len() as u16).to_be_bytes().to_vec();
    let mut a_key = arg.to_be_bytes().to_vec();
    a_len.append(&mut a_string);
    a_len.append(&mut a_key);
    a_len
}

fn get_join_key(from: String) -> Vec<u8> {
    let mut a_string = from.as_bytes().to_vec();
    let mut a_len = (a_string.len() as u16).to_be_bytes().to_vec();
    a_len.append(&mut a_string);
    a_len
}
