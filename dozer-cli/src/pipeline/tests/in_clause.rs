use std::collections::HashMap;
use dozer_cli::pipeline::in_clause::evaluate_in;

/// Simple unit test for the `evaluate_in` helper that mirrors the behaviour
/// of the newly added `IN` clause support in the query builder.
#[test]
fn test_evaluate_in_helper() {
    // Record with a single column `id`.
    let mut record = HashMap::new();
    record.insert("id".to_string(), "2".to_string());

    // Positive case – the value is present in the list.
    let allowed = vec!["1".to_string(), "2".to_string(), "3".to_string()];
    assert!(evaluate_in(&record, "id", &allowed));

    // Negative case – the value is not present in the list.
    let not_allowed = vec!["4".to_string(), "5".to_string()];
    assert!(!evaluate_in(&record, "id", &not_allowed));
}
