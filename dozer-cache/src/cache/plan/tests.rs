use super::QueryPlanner;
use crate::cache::{
    expression::{self, ExecutionStep, FilterExpression, QueryExpression},
    test_utils,
};

use dozer_types::serde_json::Value;

#[test]
fn test_generate_plan_simple() {
    let schema = test_utils::schema_0();
    let planner = QueryPlanner {};
    let query = QueryExpression::new(
        Some(FilterExpression::Simple(
            "foo".to_string(),
            expression::Operator::EQ,
            Value::from("bar".to_string()),
        )),
        vec![],
        10,
        0,
    );
    if let ExecutionStep::IndexScan(index_scan) = planner.plan(&schema, &query).unwrap() {
        assert_eq!(index_scan.index_def, schema.secondary_indexes[0]);
        assert_eq!(index_scan.fields, &[Some(Value::from("bar".to_string()))]);
    } else {
        panic!("IndexScan expected")
    }
}

#[test]
fn test_generate_plan_and() {
    let schema = test_utils::schema_1();
    let planner = QueryPlanner {};

    let filter = FilterExpression::And(vec![
        FilterExpression::Simple("a".to_string(), expression::Operator::EQ, Value::from(1)),
        FilterExpression::Simple(
            "b".to_string(),
            expression::Operator::EQ,
            Value::from("test".to_string()),
        ),
    ]);
    let query = QueryExpression::new(Some(filter), vec![], 10, 0);
    // Pick the 3rd index
    if let ExecutionStep::IndexScan(index_scan) = planner.plan(&schema, &query).unwrap() {
        assert_eq!(index_scan.index_def, schema.secondary_indexes[3]);
        assert_eq!(
            index_scan.fields,
            &[Some(Value::from(1)), Some(Value::from("test".to_string()))]
        );
    } else {
        panic!("IndexScan expected")
    }
}
