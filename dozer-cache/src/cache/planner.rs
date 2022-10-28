use dozer_types::{
    errors::cache::PlanError,
    types::{FieldIndexAndDirection, SortDirection},
};

use crate::cache::expression::{FilterExpression, IndexScan, Operator, QueryExpression, SeqScan};
use dozer_types::types::{FieldDefinition, IndexDefinition, Schema};

use super::expression::{PlanResult, SimpleFilterExpression};

pub struct QueryPlanner {}
impl QueryPlanner {
    pub fn plan(&self, schema: &Schema, query: &QueryExpression) -> Result<PlanResult, PlanError> {
        // Collect filter expressions.
        // There're four "passes" of collecting:
        // 1. Classify all filter expressions as range filters, full text filters and `Eq` filters.
        // 2. For each range filter, find an index that contains the specified field at last, and whose all other fields appear in an `Eq` filter.
        // 3. For all the `Eq` filters that are not covered in step 2, find an index that contains it, and whose all fields before this field appear in an `Eq` filter.
        // 4. For each full text filter, find an index that contains the specified field.
        // Step 1.
        let mut range_filters = vec![];
        let mut full_text_filters = vec![];
        let mut eq_filters = vec![];
        if let Some(filter_expression) = &query.filter {
            classify_filters(
                filter_expression,
                &schema.fields,
                &mut range_filters,
                &mut full_text_filters,
                &mut eq_filters,
            )?;
        }
        // Step 2.
        for filter in range_filters.iter_mut() {
            for index in &schema.secondary_indexes {
                if try_using_index_for_range_filter(index, filter, &mut eq_filters) {
                    break;
                }
            }
        }
        // Step 3.
        for eq_filter_index in 0..eq_filters.len() {
            for index in &schema.secondary_indexes {
                if try_using_index_for_eq_filter(index, &mut eq_filters, eq_filter_index) {
                    break;
                }
            }
        }
        // Step 4.
        for filter in full_text_filters.iter_mut() {
            for index in &schema.secondary_indexes {
                if can_be_used_for_full_text_filter(index, filter.field_index) {
                    filter.index_scan = Some(IndexScan {
                        index_def: index.clone(),
                        filters: vec![filter.filter.clone()],
                    });
                    break;
                }
            }
        }

        if !query.order_by.is_empty() {
            todo!("Handle order by");
        }

        // Collect `IndexScan`s.
        let mut index_scans = vec![];
        for filter in range_filters {
            if let RangeFilter::Indexed(index_scan) = filter {
                index_scans.push(index_scan);
            } else {
                return Err(PlanError::NeedIndex);
            }
        }
        for filter in eq_filters {
            match filter.state {
                EqFilterState::IndexedWithEqFilter {
                    index_scan: Some(index_scan),
                } => index_scans.push(index_scan),
                EqFilterState::NotIndexedWithEqFilter {
                    indexed_with_range_filter: false,
                    ..
                } => return Err(PlanError::NeedIndex),
                _ => (),
            }
        }
        for filter in full_text_filters {
            if let Some(index_scan) = filter.index_scan {
                index_scans.push(index_scan);
            } else {
                return Err(PlanError::NeedIndex);
            }
        }

        if index_scans.is_empty() {
            Ok(PlanResult::SeqScan(SeqScan {
                direction: SortDirection::Ascending,
            }))
        } else {
            Ok(PlanResult::IndexScans(index_scans))
        }
    }
}

fn get_field_index(field_name: &str, fields: &[FieldDefinition]) -> Option<usize> {
    fields.iter().position(|f| f.name == field_name)
}

#[derive(Debug)]
enum RangeFilter {
    Unindexed {
        field_index: usize,
        filter: SimpleFilterExpression,
    },
    Indexed(IndexScan),
}

#[derive(Debug)]
struct EqFilter {
    filter: SimpleFilterExpression,
    state: EqFilterState,
}

#[derive(Debug)]
enum EqFilterState {
    NotIndexedWithEqFilter {
        field_index: usize,
        indexed_with_range_filter: bool,
    },
    IndexedWithEqFilter {
        // For all the eq filters to be queried using a common `IndexScan`, only one of `index_scan` is some.
        index_scan: Option<IndexScan>,
    },
}

#[derive(Debug)]
struct FullTextFilter {
    field_index: usize,
    filter: SimpleFilterExpression,
    index_scan: Option<IndexScan>,
}

fn range_filter_contains_filed_index(filters: &[RangeFilter], index: usize) -> bool {
    for filter in filters {
        if let RangeFilter::Unindexed { field_index, .. } = filter {
            if *field_index == index {
                return true;
            }
        } else {
            debug_assert!(false, "Range filter must be unindexed at this time");
        }
    }
    false
}

fn eq_filter_contains_field_index(filters: &[EqFilter], index: usize) -> bool {
    for filter in filters {
        if let EqFilterState::NotIndexedWithEqFilter {
            field_index,
            indexed_with_range_filter: false,
        } = &filter.state
        {
            if *field_index == index {
                return true;
            }
        } else {
            debug_assert!(false, "Eq filter must be unindexed at this time");
        }
    }
    false
}

fn classify_filters(
    filter_expression: &FilterExpression,
    fields: &[FieldDefinition],
    range_filters: &mut Vec<RangeFilter>,
    full_text_filters: &mut Vec<FullTextFilter>,
    eq_filters: &mut Vec<EqFilter>,
) -> Result<(), PlanError> {
    match filter_expression {
        FilterExpression::Simple(simple) => {
            let field_index = match get_field_index(&simple.field_name, fields) {
                Some(index) => index,
                None => return Err(PlanError::CannotFindField(simple.field_name.clone())),
            };
            // Intentionally not using set here because the size of the vec should be small.
            match simple.operator {
                Operator::LT | Operator::LTE | Operator::GT | Operator::GTE => {
                    if !range_filter_contains_filed_index(range_filters, field_index) {
                        range_filters.push(RangeFilter::Unindexed {
                            field_index,
                            filter: simple.clone(),
                        });
                    }
                }
                Operator::Contains | Operator::MatchesAll | Operator::MatchesAny => {
                    if !full_text_filters
                        .iter()
                        .any(|filter| filter.field_index == field_index)
                    {
                        full_text_filters.push(FullTextFilter {
                            field_index,
                            filter: simple.clone(),
                            index_scan: None,
                        });
                    }
                }
                Operator::EQ => {
                    if !eq_filter_contains_field_index(eq_filters, field_index) {
                        eq_filters.push(EqFilter {
                            filter: simple.clone(),
                            state: EqFilterState::NotIndexedWithEqFilter {
                                field_index,
                                indexed_with_range_filter: false,
                            },
                        });
                    }
                }
            }
        }
        FilterExpression::And(expressions) => {
            for expression in expressions {
                classify_filters(
                    expression,
                    fields,
                    range_filters,
                    full_text_filters,
                    eq_filters,
                )?;
            }
        }
    }
    Ok(())
}

fn collect_eq_filters(
    fields: &[FieldIndexAndDirection],
    filters: &[EqFilter],
) -> Option<Vec<usize>> {
    let mut result = vec![];
    for field in fields {
        if let Some((eq_filter_index, _)) = filters.iter().enumerate().find(|(_, filter)| {
            if let EqFilterState::NotIndexedWithEqFilter { field_index, .. } = &filter.state {
                *field_index == field.index
            } else {
                debug_assert!(
                    false,
                    "Eq filter must be not indexed with eq filter at this time"
                );
                false
            }
        }) {
            result.push(eq_filter_index);
        } else {
            return None;
        }
    }
    Some(result)
}

fn try_using_index_for_range_filter(
    index: &IndexDefinition,
    range_filter: &mut RangeFilter,
    eq_filters: &mut [EqFilter],
) -> bool {
    if let IndexDefinition::SortedInverted { fields } = index {
        if let Some(field) = fields.last() {
            if let RangeFilter::Unindexed {
                field_index,
                filter,
            } = range_filter
            {
                if field.index == *field_index {
                    if let Some(eq_filter_indices) =
                        collect_eq_filters(&fields[..fields.len() - 1], eq_filters)
                    {
                        let mut filters = vec![];
                        // Update and add `Eq` filters.
                        for eq_filter_index in eq_filter_indices {
                            let eq_filter = &mut eq_filters[eq_filter_index];
                            match &mut eq_filter.state {
                                EqFilterState::NotIndexedWithEqFilter { indexed_with_range_filter, .. } => {
                                    *indexed_with_range_filter = true;
                                },
                                EqFilterState::IndexedWithEqFilter { .. } => unreachable!("No Eq filter should be indexed with other Eq filters at this time"),
                            };
                            filters.push(eq_filter.filter.clone());
                        }
                        // Add range filter.
                        filters.push(filter.clone());
                        // Update range filter.
                        debug_assert!(filters.len() == fields.len());
                        *range_filter = RangeFilter::Indexed(IndexScan {
                            index_def: index.clone(),
                            filters,
                        });
                        return true;
                    }
                }
            } else {
                debug_assert!(false, "Range filter must be unindexed at this time");
            }
        }
    }
    false
}

fn try_using_index_for_eq_filter(
    index: &IndexDefinition,
    eq_filters: &mut [EqFilter],
    eq_filter_index: usize,
) -> bool {
    if let EqFilterState::NotIndexedWithEqFilter {
        field_index,
        indexed_with_range_filter: false,
        ..
    } = &eq_filters[eq_filter_index].state
    {
        let field_index = *field_index;
        if let IndexDefinition::SortedInverted { fields } = index {
            // Check if this index contains current filter's field.
            if fields.iter().any(|field| field.index == field_index) {
                // Check if all this index's fields are filtered by `Eq` filters.
                if let Some(eq_filter_indices) = collect_eq_filters(fields, eq_filters) {
                    // Collect all filters of this index's fields and update filter state.
                    let mut filters = vec![];
                    for eq_filter_index in eq_filter_indices {
                        let filter = &mut eq_filters[eq_filter_index];
                        filter.state = EqFilterState::IndexedWithEqFilter { index_scan: None };
                        filters.push(filter.filter.clone());
                    }
                    // Only current filter contains an `IndexScan`.
                    eq_filters[eq_filter_index].state = EqFilterState::IndexedWithEqFilter {
                        index_scan: Some(IndexScan {
                            index_def: index.clone(),
                            filters,
                        }),
                    };
                    return true;
                }
            }
        }
        false
    } else {
        true
    }
}

fn can_be_used_for_full_text_filter(index: &IndexDefinition, filter_field_index: usize) -> bool {
    if let IndexDefinition::FullText { field_index } = index {
        *field_index == filter_field_index
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::QueryPlanner;
    use crate::cache::{
        expression::{self, FilterExpression, PlanResult, QueryExpression, SimpleFilterExpression},
        test_utils,
    };

    use dozer_types::serde_json::Value;

    #[test]
    fn test_generate_plan_simple() {
        let schema = test_utils::schema_0();
        let planner = QueryPlanner {};
        let filter = SimpleFilterExpression {
            field_name: "foo".to_string(),
            operator: expression::Operator::EQ,
            value: Value::from("bar".to_string()),
        };
        let query = QueryExpression::new(
            Some(FilterExpression::Simple(filter.clone())),
            vec![],
            10,
            0,
        );
        if let PlanResult::IndexScans(index_scans) = planner.plan(&schema, &query).unwrap() {
            assert_eq!(index_scans.len(), 1);
            assert_eq!(index_scans[0].index_def, schema.secondary_indexes[0]);
            assert_eq!(index_scans[0].filters, &[filter]);
        } else {
            panic!("IndexScan expected")
        }
    }

    #[test]
    fn test_generate_plan_and() {
        let schema = test_utils::schema_1();
        let planner = QueryPlanner {};

        let filters = vec![
            SimpleFilterExpression {
                field_name: "a".to_string(),
                operator: expression::Operator::EQ,
                value: Value::from(1),
            },
            SimpleFilterExpression {
                field_name: "b".to_string(),
                operator: expression::Operator::EQ,
                value: Value::from("test".to_string()),
            },
        ];
        let filter = FilterExpression::And(
            filters
                .iter()
                .map(|filter| FilterExpression::Simple(filter.clone()))
                .collect(),
        );
        let query = QueryExpression::new(Some(filter), vec![], 10, 0);
        // Pick the 3rd index
        if let PlanResult::IndexScans(index_scans) = planner.plan(&schema, &query).unwrap() {
            assert_eq!(index_scans.len(), 1);
            assert_eq!(index_scans[0].index_def, schema.secondary_indexes[3]);
            assert_eq!(index_scans[0].filters, filters,);
        } else {
            panic!("IndexScan expected")
        }
    }
}
