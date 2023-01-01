use std::cmp::Ordering;

use crate::pipeline::product::key_comparator::compare_join_index;

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
