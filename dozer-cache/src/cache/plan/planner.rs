use crate::cache::expression::{FilterExpression, Operator, SortDirection, SortOptions};
use crate::errors::PlanError;
use dozer_types::models::api_endpoint::{
    CreateSecondaryIndex, FullText, SecondaryIndex, SortedInverted,
};
use dozer_types::types::{Field, FieldDefinition, Schema};
use dozer_types::types::{FieldType, IndexDefinition};
use dozer_types::{json_value_to_field, serde_yaml};

use super::helper::{RangeQuery, RangeQueryKind};
use super::{helper, IndexScan, Plan, SeqScan};
use super::{IndexFilter, IndexScanKind};

pub struct QueryPlanner<'a> {
    schema: &'a Schema,
    secondary_indexes: &'a [IndexDefinition],
    filter: Option<&'a FilterExpression>,
    order_by: &'a SortOptions,
}
impl<'a> QueryPlanner<'a> {
    pub fn new(
        schema: &'a Schema,
        secondary_indexes: &'a [IndexDefinition],
        filter: Option<&'a FilterExpression>,
        order_by: &'a SortOptions,
    ) -> Self {
        Self {
            schema,
            secondary_indexes,
            filter,
            order_by,
        }
    }

    pub fn plan(&self) -> Result<Plan, PlanError> {
        // Collect all the filters.
        // TODO: Handle filters like And([a > 0, a < 10]).
        let mut filters = vec![];
        if let Some(expression) = &self.filter {
            collect_filters(self.schema, expression, &mut filters)?;
        }

        // Filter the sort options.
        // TODO: Handle duplicate fields.
        let mut order_by = vec![];
        for order in &self.order_by.0 {
            // Find the field index.
            let (field_index, _, _) =
                get_field_index_and_type(&order.field_name, &self.schema.fields)
                    .ok_or_else(|| PlanError::FieldNotFound(order.field_name.clone()))?;
            // If the field is already in a filter supported by `SortedInverted`, mark the corresponding filter.
            if seen_in_sorted_inverted_filter(field_index, order.direction, &mut filters)? {
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

        // If non-`Eq` filter is applied to `null` value, return empty result.
        if filters
            .iter()
            .any(|f| matches!(f.0.val, Field::Null) && f.0.op != Operator::EQ)
        {
            return Ok(Plan::ReturnEmpty);
        }

        // Find the range query, can be a range filter or a sort option.
        let range_query = find_range_query(&mut filters, &order_by)?;

        // Generate some index scans that can answer this query, lazily.
        let all_index_scans = helper::get_all_indexes(filters, range_query);

        // Check if existing secondary indexes can satisfy any of the scans.
        let mut scans = None;
        for index_scans in all_index_scans {
            if scans.is_none() {
                scans = Some(index_scans.clone());
            }

            if let Some(index_scans) = all_indexes_are_present(self.secondary_indexes, index_scans)
            {
                return Ok(Plan::IndexScans(index_scans));
            }
        }

        Err(PlanError::MatchingIndexNotFound(
            describe_index_configuration(
                &self.schema.fields,
                &scans.expect("Planner should always generate plan"),
            ),
        ))
    }
}

fn get_field_index_and_type(
    field_name: &str,
    fields: &[FieldDefinition],
) -> Option<(usize, FieldType, bool)> {
    fields
        .iter()
        .enumerate()
        .find(|(_, f)| f.name == field_name)
        .map(|(i, f)| (i, f.typ, f.nullable))
}

fn collect_filters(
    schema: &Schema,
    expression: &FilterExpression,
    filters: &mut Vec<(IndexFilter, Option<SortDirection>)>,
) -> Result<(), PlanError> {
    match expression {
        FilterExpression::Simple(field_name, operator, value) => {
            let (field_index, field_type, nullable) =
                get_field_index_and_type(field_name, &schema.fields)
                    .ok_or_else(|| PlanError::FieldNotFound(field_name.clone()))?;
            let field = json_value_to_field(value.clone(), field_type, nullable)?;
            filters.push((IndexFilter::new(field_index, *operator, field), None));
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
    sort_direction: SortDirection,
    filters: &mut [(IndexFilter, Option<SortDirection>)],
) -> Result<bool, PlanError> {
    for filter in filters {
        if filter.0.field_index == field_index {
            return if !filter.0.op.supported_by_sorted_inverted() {
                Err(PlanError::CannotSortFullTextFilter)
            } else if let Some(direction) = filter.1 {
                if direction == sort_direction {
                    Ok(true)
                } else {
                    Err(PlanError::ConflictingSortOptions)
                }
            } else {
                filter.1 = Some(sort_direction);
                Ok(true)
            };
        }
    }

    Ok(false)
}

fn find_range_query(
    filters: &mut Vec<(IndexFilter, Option<SortDirection>)>,
    order_by: &[(usize, SortDirection)],
) -> Result<Option<RangeQuery>, PlanError> {
    let mut num_range_ops = 0;
    let mut range_filter_index = None;
    for (i, filter) in filters.iter().enumerate() {
        if filter.0.op.is_range_operator() {
            num_range_ops += 1;
            range_filter_index = Some(i);
        }
    }
    num_range_ops += order_by.len();
    if num_range_ops > 1 {
        return Err(PlanError::RangeQueryLimit);
    }
    Ok(if let Some(range_filter_index) = range_filter_index {
        let filter = filters.remove(range_filter_index);
        Some(RangeQuery::new(
            filter.0.field_index,
            RangeQueryKind::Filter {
                operator: filter.0.op,
                value: filter.0.val,
                sort_direction: filter.1,
            },
        ))
    } else if let Some((field_index, sort_direction)) = order_by.first() {
        Some(RangeQuery::new(
            *field_index,
            RangeQueryKind::OrderBy {
                sort_direction: *sort_direction,
            },
        ))
    } else {
        None
    })
}

impl IndexScanKind {
    fn is_supported_by_index(&self, index: &IndexDefinition) -> bool {
        match (self, index) {
            (
                IndexScanKind::SortedInverted {
                    eq_filters,
                    range_query,
                },
                IndexDefinition::SortedInverted(fields),
            ) => {
                if fields.len() < eq_filters.len() {
                    return false;
                }
                if !eq_filters
                    .iter()
                    .zip(fields)
                    .all(|(filter, field)| filter.0 == *field)
                {
                    return false;
                }
                if let Some(range_query) = range_query {
                    if fields.len() != eq_filters.len() + 1 {
                        return false;
                    }
                    let last_field = fields
                        .last()
                        .expect("We've checked `fields.len()` is at least 1");
                    range_query.field_index == *last_field
                } else {
                    fields.len() == eq_filters.len()
                }
            }
            (IndexScanKind::FullText { filter }, IndexDefinition::FullText(field_index)) => {
                filter.field_index == *field_index
            }
            _ => false,
        }
    }
}

fn all_indexes_are_present(
    indexes: &[IndexDefinition],
    index_scan_kinds: Vec<IndexScanKind>,
) -> Option<Vec<IndexScan>> {
    let mut scans = vec![];
    for index_scan_kind in index_scan_kinds {
        let found: Option<(usize, &IndexDefinition)> = indexes
            .iter()
            .enumerate()
            .find(|(_, i)| index_scan_kind.is_supported_by_index(i));

        match found {
            Some((idx, _)) => {
                scans.push(IndexScan {
                    index_id: idx,
                    kind: index_scan_kind,
                });
            }
            None => return None,
        }
    }
    Some(scans)
}

fn describe_index_configuration(
    field_definitions: &[FieldDefinition],
    indexes: &[IndexScanKind],
) -> String {
    let mut creates = vec![];
    for index in indexes {
        match index {
            IndexScanKind::FullText { filter } => {
                let field = field_definitions[filter.field_index].name.clone();
                creates.push(CreateSecondaryIndex {
                    index: Some(SecondaryIndex::FullText(FullText { field })),
                });
            }
            IndexScanKind::SortedInverted {
                eq_filters,
                range_query,
            } => {
                let mut fields = vec![];
                for (field_index, _) in eq_filters {
                    let field = field_definitions[*field_index].name.clone();
                    fields.push(field);
                }
                if let Some(range_query) = range_query {
                    let field = field_definitions[range_query.field_index].name.clone();
                    fields.push(field);
                }
                creates.push(CreateSecondaryIndex {
                    index: Some(SecondaryIndex::SortedInverted(SortedInverted { fields })),
                });
            }
        }
    }

    serde_yaml::to_string(&creates).expect("This serialization should never fail.")
}

#[cfg(test)]
mod tests {
    use crate::cache::plan::SortedInvertedRangeQuery;

    use super::*;

    #[test]
    fn test_is_supported_by_index() {
        let check_sorted_inverted =
            |eq_filters: Vec<usize>, range_query: Option<usize>, index, expected: bool| {
                assert_eq!(
                    IndexScanKind::SortedInverted {
                        eq_filters: eq_filters
                            .into_iter()
                            .map(|index| (index, Field::Null))
                            .collect(),
                        range_query: range_query.map(|index| SortedInvertedRangeQuery {
                            field_index: index,
                            sort_direction: SortDirection::Ascending,
                            operator_and_value: None,
                        })
                    }
                    .is_supported_by_index(&IndexDefinition::SortedInverted(index)),
                    expected
                );
            };

        check_sorted_inverted(vec![0], None, vec![0], true);
        check_sorted_inverted(vec![0], None, vec![1], false);
        check_sorted_inverted(vec![0], None, vec![0, 1], false);
        check_sorted_inverted(vec![0, 1], None, vec![0], false);
        check_sorted_inverted(vec![0, 1], None, vec![0, 1], true);
        check_sorted_inverted(vec![], Some(0), vec![0], true);
        check_sorted_inverted(vec![0], Some(1), vec![0, 1], true);
        check_sorted_inverted(vec![0], Some(1), vec![0, 1, 2], false);
        check_sorted_inverted(vec![0], Some(1), vec![0, 2], false);
        check_sorted_inverted(vec![0], Some(1), vec![0], false);

        let full_text_scan = IndexScanKind::FullText {
            filter: IndexFilter {
                field_index: 0,
                op: Operator::Contains,
                val: Field::Null,
            },
        };
        assert!(full_text_scan.is_supported_by_index(&IndexDefinition::FullText(0)),);
        assert!(!full_text_scan.is_supported_by_index(&IndexDefinition::FullText(1)));

        assert!(!full_text_scan.is_supported_by_index(&IndexDefinition::SortedInverted(vec![0])),);
        assert!(!IndexScanKind::SortedInverted {
            eq_filters: vec![(0, Field::Null)],
            range_query: None
        }
        .is_supported_by_index(&IndexDefinition::FullText(0)),);
    }
}
