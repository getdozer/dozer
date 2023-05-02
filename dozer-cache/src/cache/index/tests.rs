use dozer_types::types::field_test_cases;

use crate::cache::index::{get_composite_secondary_index, CompositeSecondaryIndexKey};

use super::get_full_text_secondary_index;

#[test]
fn test_get_full_text_secondary_index() {
    assert_eq!(get_full_text_secondary_index("foo"), b"foo",);
}

#[test]
fn test_composite_key_encode_roundtrip() {
    // Single field
    for field in field_test_cases() {
        let key = get_composite_secondary_index(&[&field]);
        let mut key = CompositeSecondaryIndexKey::new(&key);
        assert_eq!(key.next().unwrap().unwrap(), field);
        assert!(key.next().is_none());
    }

    // Two fields
    for field1 in field_test_cases() {
        for field2 in field_test_cases() {
            let key = get_composite_secondary_index(&[&field1, &field2]);
            let mut key = CompositeSecondaryIndexKey::new(&key);
            assert_eq!(key.next().unwrap().unwrap(), field1);
            assert_eq!(key.next().unwrap().unwrap(), field2);
            assert!(key.next().is_none());
        }
    }
}

#[test]
fn composite_key_encode_preserves_prefix() {
    for field1 in field_test_cases() {
        for field2 in field_test_cases() {
            let key1 = get_composite_secondary_index(&[&field1]);
            let key2 = get_composite_secondary_index(&[&field1, &field2]);
            assert_eq!(key2[..key1.len()], key1);
        }
    }
}
