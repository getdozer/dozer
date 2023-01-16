use super::{Plan, QueryPlanner};
use crate::cache::{
    expression::{self, FilterExpression, Operator, QueryExpression, SortDirection, SortOption},
    plan::{IndexScanKind, SortedInvertedRangeQuery},
    test_utils,
};

use dozer_types::{serde_json::Value, types::Field};

#[test]
fn test_generate_plan_simple() {
    let (schema, secondary_indexes) = test_utils::schema_0();

    let query = QueryExpression::new(
        Some(FilterExpression::Simple(
            "foo".to_string(),
            expression::Operator::EQ,
            Value::from("bar".to_string()),
        )),
        vec![],
        Some(10),
        0,
    );
    let planner = QueryPlanner::new(&schema, &secondary_indexes, &query);
    if let Plan::IndexScans(index_scans) = planner.plan().unwrap() {
        assert_eq!(index_scans.len(), 1);
        assert_eq!(index_scans[0].index_id, 0);
        match &index_scans[0].kind {
            IndexScanKind::SortedInverted {
                eq_filters,
                range_query,
            } => {
                assert_eq!(eq_filters.len(), 1);
                assert_eq!(eq_filters[0], (0, Field::String("bar".to_string())));
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
    let (schema, secondary_indexes) = test_utils::schema_1();

    let filter = FilterExpression::And(vec![
        FilterExpression::Simple("a".to_string(), expression::Operator::EQ, Value::from(1)),
        FilterExpression::Simple(
            "b".to_string(),
            expression::Operator::EQ,
            Value::from("test".to_string()),
        ),
    ]);
    let query = QueryExpression::new(Some(filter), vec![], Some(10), 0);
    let planner = QueryPlanner::new(&schema, &secondary_indexes, &query);
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
                assert_eq!(eq_filters[0], (0, Field::Int(1)));
                assert_eq!(eq_filters[1], (1, Field::String("test".to_string())));
                assert_eq!(range_query, &None);
            }
            _ => panic!("Must be sorted inverted"),
        }
    } else {
        panic!("IndexScan expected")
    }
}

#[test]
fn test_generate_plan_range_query_and_order_by() {
    let (schema, secondary_indexes) = test_utils::schema_1();
    let filter = FilterExpression::Simple("c".into(), expression::Operator::GT, 1.into());
    let query = QueryExpression::new(
        Some(filter),
        vec![SortOption {
            field_name: "c".into(),
            direction: SortDirection::Descending,
        }],
        Some(10),
        0,
    );
    let planner = QueryPlanner::new(&schema, &secondary_indexes, &query);
    if let Plan::IndexScans(index_scans) = planner.plan().unwrap() {
        assert_eq!(index_scans.len(), 1);
        assert_eq!(index_scans[0].index_id, 2);
        match &index_scans[0].kind {
            IndexScanKind::SortedInverted {
                eq_filters,
                range_query,
            } => {
                assert_eq!(eq_filters.len(), 0);
                assert_eq!(
                    range_query,
                    &Some(SortedInvertedRangeQuery {
                        field_index: 2,
                        sort_direction: SortDirection::Descending,
                        operator_and_value: Some((expression::Operator::GT, 1.into())),
                    })
                );
            }
            _ => panic!("Must be sorted inverted"),
        }
    } else {
        panic!("IndexScan expected")
    }
}

#[test]
fn test_generate_plan_empty() {
    let (schema, secondary_indexes) = test_utils::schema_1();

    let query = QueryExpression::new(
        Some(FilterExpression::Simple(
            "c".into(),
            Operator::LT,
            Value::Null,
        )),
        vec![],
        Some(10),
        0,
    );
    let planner = QueryPlanner::new(&schema, &secondary_indexes, &query);
    assert!(matches!(planner.plan().unwrap(), Plan::ReturnEmpty));
}
