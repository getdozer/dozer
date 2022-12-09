use super::get_full_text_secondary_index;

#[test]
fn secondary_index_is_never_empty() {
    assert!(!super::get_secondary_index(&[], false).unwrap().is_empty());
}

#[test]
fn test_get_full_text_secondary_index() {
    assert_eq!(get_full_text_secondary_index("foo"), b"foo",);
}
