use dozer_types::{errors::cache::PlanError, types::SortDirection};

use crate::cache::expression::{
    FilterExpression, IndexScan, Operator, Plan, QueryExpression, SeqScan,
};
use dozer_types::{
    serde_json::Value,
    types::{FieldDefinition, Schema},
};

use self::helper::RangeQuery;

mod helper;

pub struct QueryPlanner {}
impl QueryPlanner {
    pub fn plan(&self, schema: &Schema, query: &QueryExpression) -> Result<Plan, PlanError> {
        // Collect all the filters.
        // TODO: Handle filters like And([a > 0, a < 10]).
        let mut filters = vec![];
        if let Some(expression) = &query.filter {
            collect_filters(schema, expression, &mut filters)?;
        }

        // Filter the sort options.
        // TODO: Handle duplicate fields.
        let mut order_by = vec![];
        for order in &query.order_by {
            if order.direction != SortDirection::Ascending {
                todo!("Support descending sort option");
            }
            // Find the field index.
            let field_index = get_field_index(&order.field_name, &schema.fields)
                .ok_or(PlanError::FieldNotFound)?;
            // If the field is already in a filter supported by `SortedInverted`, we can skip sorting it.
            if seen_in_sorted_inverted_filter(field_index, &filters) {
                continue;
            }
            // This sort option needs to be in the plan.
            order_by.push((field_index, order.direction));
        }

        // If no filter and sort is requested, return a SeqScan.
        if filters.is_empty() && order_by.is_empty() {
            return Ok(Plan::SeqScan(SeqScan {
                direction: SortDirection::Ascending,
            }));
        }

        // Find the range query, can be a range filter or a sort option.
        let range_query = find_range_query(&mut filters, &order_by)?;

        // Generate some index scans that can answer this query, lazily.
        let all_index_scans = helper::get_all_indexes(filters, range_query);

        // Check if existing secondary indexes can satisfy any of the scans.
        for index_scans in all_index_scans {
            if all_indexes_are_present(schema, &index_scans) {
                return Ok(Plan::IndexScans(index_scans));
            }
        }

        Err(PlanError::MatchingIndexNotFound)
    }
}

fn get_field_index(field_name: &str, fields: &[FieldDefinition]) -> Option<usize> {
    fields.iter().position(|f| f.name == field_name)
}

fn collect_filters(
    schema: &Schema,
    expression: &FilterExpression,
    filters: &mut Vec<(usize, Operator, Value)>,
) -> Result<(), PlanError> {
    match expression {
        FilterExpression::Simple(field_name, operator, value) => {
            let field_index =
                get_field_index(field_name, &schema.fields).ok_or(PlanError::FieldNotFound)?;
            filters.push((field_index, *operator, value.clone()));
        }
        FilterExpression::And(expressions) => {
            for expression in expressions {
                collect_filters(schema, expression, filters)?;
            }
        }
    }
    Ok(())
}

fn seen_in_sorted_inverted_filter(
    field_index: usize,
    filters: &[(usize, Operator, Value)],
) -> bool {
    filters
        .iter()
        .any(|filter| filter.0 == field_index && filter.1.supported_by_sorted_inverted())
}

fn find_range_query(
    filters: &mut Vec<(usize, Operator, Value)>,
    order_by: &[(usize, SortDirection)],
) -> Result<Option<RangeQuery>, PlanError> {
    let mut num_range_ops = 0;
    let mut range_filter_index = None;
    for (i, filter) in filters.iter().enumerate() {
        if filter.1.is_range_operator() {
            num_range_ops += 1;
            range_filter_index = Some(i);
        }
    }
    num_range_ops += order_by.len();
    if num_range_ops > 1 {
        return Err(PlanError::RangeQueryLimit);
    }
    Ok(if let Some(range_filter_index) = range_filter_index {
        let (field_index, operator, value) = filters.remove(range_filter_index);
        Some(RangeQuery {
            field_index,
            operator_and_value: Some((operator, value)),
        })
    } else if let Some((field_index, _)) = order_by.first() {
        Some(RangeQuery {
            field_index: *field_index,
            operator_and_value: None,
        })
    } else {
        None
    })
}

fn all_indexes_are_present(schema: &Schema, index_scans: &[IndexScan]) -> bool {
    index_scans.iter().all(|index_scan| {
        schema
            .secondary_indexes
            .iter()
            .any(|i| i == &index_scan.index_def)
    })
}

#[cfg(test)]
mod tests {
    use super::QueryPlanner;
    use crate::cache::{
        expression::{self, FilterExpression, Plan, QueryExpression},
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
        if let Plan::IndexScans(index_scans) = planner.plan(&schema, &query).unwrap() {
            assert_eq!(index_scans.len(), 1);
            assert_eq!(index_scans[0].index_def, schema.secondary_indexes[0]);
            assert_eq!(
                index_scans[0].fields,
                &[Some(Value::from("bar".to_string()))]
            );
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
        if let Plan::IndexScans(index_scans) = planner.plan(&schema, &query).unwrap() {
            assert_eq!(index_scans.len(), 1);
            assert_eq!(index_scans[0].index_def, schema.secondary_indexes[3]);
            assert_eq!(
                index_scans[0].fields,
                &[Some(Value::from(1)), Some(Value::from("test".to_string()))]
            );
        } else {
            panic!("IndexScan expected")
        }
    }
}
