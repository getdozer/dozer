use super::{Plan, QueryPlanner};
use crate::cache::{
    expression::{self, FilterExpression, QueryExpression},
    plan::IndexScanKind,
    test_utils,
};

use dozer_types::{
    serde_json::Value,
    types::{Field, SortDirection},
};

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
        assert_eq!(index_scans[0].index_id, 0);
        match &index_scans[0].kind {
            IndexScanKind::SortedInverted {
                eq_filters,
                range_query,
            } => {
                assert_eq!(eq_filters.len(), 1);
                assert_eq!(
                    eq_filters[0],
                    (
                        0,
                        SortDirection::Ascending,
                        Field::String("bar".to_string())
                    )
                );
                assert_eq!(range_query, &None);
            }
            _ => panic!("Must be sorted inverted"),
        }
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
        assert_eq!(index_scans[0].index_id, 3);
        match &index_scans[0].kind {
            IndexScanKind::SortedInverted {
                eq_filters,
                range_query,
            } => {
                assert_eq!(eq_filters.len(), 2);
                assert_eq!(eq_filters[0], (0, SortDirection::Ascending, Field::Int(1)));
                assert_eq!(
                    eq_filters[1],
                    (
                        1,
                        SortDirection::Ascending,
                        Field::String("test".to_string())
                    )
                );
                assert_eq!(range_query, &None);
            }
            _ => panic!("Must be sorted inverted"),
        }
    } else {
        panic!("IndexScan expected")
    }
}
