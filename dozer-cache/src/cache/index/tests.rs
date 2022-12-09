use dozer_types::types::{field_test_cases, SortDirection};

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
        for sort_direction in [SortDirection::Ascending, SortDirection::Descending] {
            let key = get_composite_secondary_index(&[(&field, sort_direction)]);
            let mut key = CompositeSecondaryIndexKey::new(&key);
            assert_eq!(
                key.next().unwrap().unwrap(),
                (field.borrow(), sort_direction)
            );
            assert!(key.next().is_none());
        }
    }

    // Two fields
    for field1 in field_test_cases() {
        for field2 in field_test_cases() {
            for sort_direction1 in [SortDirection::Ascending, SortDirection::Descending] {
                for sort_direction2 in [SortDirection::Ascending, SortDirection::Descending] {
                    let key = get_composite_secondary_index(&[
                        (&field1, sort_direction1),
                        (&field2, sort_direction2),
                    ]);
                    let mut key = CompositeSecondaryIndexKey::new(&key);
                    assert_eq!(
                        key.next().unwrap().unwrap(),
                        (field1.borrow(), sort_direction1)
                    );
                    assert_eq!(
                        key.next().unwrap().unwrap(),
                        (field2.borrow(), sort_direction2)
                    );
                    assert!(key.next().is_none());
                }
            }
        }
    }
}

#[test]
fn composite_key_encode_preserves_prefix() {
    for field1 in field_test_cases() {
        for field2 in field_test_cases() {
            for sort_direction1 in [SortDirection::Ascending, SortDirection::Descending] {
                for sort_direction2 in [SortDirection::Ascending, SortDirection::Descending] {
                    let key1 = get_composite_secondary_index(&[(&field1, sort_direction1)]);
                    let key2 = get_composite_secondary_index(&[
                        (&field1, sort_direction1),
                        (&field2, sort_direction2),
                    ]);
                    assert_eq!(key2[..key1.len()], key1);
                }
            }
        }
    }
}
