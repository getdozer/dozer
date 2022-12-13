use super::SortDirection;

#[test]
fn test_sort_direction_serialization() {
    assert_eq!(
        SortDirection::from_u8(SortDirection::Ascending.to_u8()).unwrap(),
        SortDirection::Ascending
    );
    assert_eq!(
        SortDirection::from_u8(SortDirection::Descending.to_u8()).unwrap(),
        SortDirection::Descending
    );
    assert!(SortDirection::from_u8(3).is_none());
}
