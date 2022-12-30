use crate::pipeline::expression::builder::compare_name;

#[test]
fn test_compare_name() {
    let left_identifier = "user.name".to_string();
    let right_identifier = "name".to_string();

    let is_equal = compare_name(left_identifier, right_identifier);
    assert_eq!(is_equal, true);
}
