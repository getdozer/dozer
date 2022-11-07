use super::{Plan, QueryPlanner};
use crate::cache::{
    expression::{self, FilterExpression, QueryExpression},
    plan::IndexFilter,
    test_utils,
};

use dozer_types::serde_json::Value;

#[test]
fn test_generate_plan_simple() {
    let schema = test_utils::schema_0();

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
    let planner = QueryPlanner::new(&schema, &query);
    if let Plan::IndexScans(index_scans) = planner.plan().unwrap() {
        assert_eq!(index_scans.len(), 1);
        assert_eq!(index_scans[0].index_def, schema.secondary_indexes[0]);
        assert_eq!(
            index_scans[0].filters,
            vec![Some(IndexFilter::equals(Value::from("bar".to_string())))]
        );
    } else {
        panic!("IndexScan expected")
    }
}

#[test]
fn test_generate_plan_and() {
    let schema = test_utils::schema_1();

    let filter = FilterExpression::And(vec![
        FilterExpression::Simple("a".to_string(), expression::Operator::EQ, Value::from(1)),
        FilterExpression::Simple(
            "b".to_string(),
            expression::Operator::EQ,
            Value::from("test".to_string()),
        ),
    ]);
    let query = QueryExpression::new(Some(filter), vec![], 10, 0);
    let planner = QueryPlanner::new(&schema, &query);
    // Pick the 3rd index
    if let Plan::IndexScans(index_scans) = planner.plan().unwrap() {
        assert_eq!(index_scans.len(), 1);
        assert_eq!(index_scans[0].index_def, schema.secondary_indexes[3]);
        assert_eq!(
            index_scans[0].filters,
            vec![
                Some(IndexFilter::new(expression::Operator::EQ, Value::from(1))),
                Some(IndexFilter::equals(Value::from("test".to_string())))
            ]
        );
    } else {
        panic!("IndexScan expected")
    }
}
