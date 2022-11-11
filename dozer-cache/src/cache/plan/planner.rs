use crate::cache::expression::{FilterExpression, QueryExpression};
use crate::errors::PlanError;
use dozer_types::json_value_to_field;
use dozer_types::types::{FieldDefinition, Schema};
use dozer_types::types::{FieldType, IndexDefinition, SortDirection};

use super::helper::{RangeQuery, RangeQueryKind};
use super::{helper, IndexScan, Plan, SeqScan};
use super::{IndexFilter, IndexScanKind};

pub struct QueryPlanner<'a> {
    schema: &'a Schema,
    query: &'a QueryExpression,
}
impl<'a> QueryPlanner<'a> {
    pub fn new(schema: &'a Schema, query: &'a QueryExpression) -> Self {
        Self { schema, query }
    }

    pub fn plan(&self) -> Result<Plan, PlanError> {
        // Collect all the filters.
        // TODO: Handle filters like And([a > 0, a < 10]).
        let mut filters = vec![];
        if let Some(expression) = &self.query.filter {
            collect_filters(self.schema, expression, &mut filters)?;
        }

        // Filter the sort options.
        // TODO: Handle duplicate fields.
        let mut order_by = vec![];
        for order in &self.query.order_by {
            // Find the field index.
            let (field_index, _) = get_field_index_and_type(&order.field_name, &self.schema.fields)
                .ok_or(PlanError::FieldNotFound)?;
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

        // Find the range query, can be a range filter or a sort option.
        let range_query = find_range_query(&mut filters, &order_by)?;

        // Generate some index scans that can answer this query, lazily.
        let all_index_scans = helper::get_all_indexes(filters, range_query);

        // Check if existing secondary indexes can satisfy any of the scans.
        for index_scans in all_index_scans {
            if let Some(index_scans) = all_indexes_are_present(self.schema, index_scans) {
                return Ok(Plan::IndexScans(index_scans));
            }
        }

        Err(PlanError::MatchingIndexNotFound)
    }
}

fn get_field_index_and_type(
    field_name: &str,
    fields: &[FieldDefinition],
) -> Option<(usize, FieldType)> {
    fields
        .iter()
        .enumerate()
        .find(|(_, f)| f.name == field_name)
        .map(|(i, f)| (i, f.typ))
}

fn collect_filters(
    schema: &Schema,
    expression: &FilterExpression,
    filters: &mut Vec<(IndexFilter, Option<SortDirection>)>,
) -> Result<(), PlanError> {
    match expression {
        FilterExpression::Simple(field_name, operator, value) => {
            let (field_index, field_type) = get_field_index_and_type(field_name, &schema.fields)
                .ok_or(PlanError::FieldNotFound)?;
            let field =
                json_value_to_field(value.as_str().unwrap_or(&value.to_string()), &field_type)?;
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
                    .all(|(filter, field)| filter.0 == field.0 && filter.1 == field.1)
                {
                    return false;
                }
                if let Some(range_query) = range_query {
                    if fields.len() != eq_filters.len() + 1 {
                        return false;
                    }
                    let last_field = fields.last().unwrap();
                    range_query.field_index == last_field.0
                        && range_query.sort_direction == last_field.1
                } else {
                    true
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
    schema: &Schema,
    index_scan_kinds: Vec<IndexScanKind>,
) -> Option<Vec<IndexScan>> {
    let mut scans = vec![];
    for index_scan_kind in index_scan_kinds {
        let found = schema
            .secondary_indexes
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
