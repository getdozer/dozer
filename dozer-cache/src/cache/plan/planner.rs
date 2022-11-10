use crate::cache::expression::{FilterExpression, QueryExpression};
use crate::errors::PlanError;
use dozer_types::json_value_to_field;
use dozer_types::types::{FieldDefinition, Schema};
use dozer_types::types::{FieldType, IndexDefinition, SortDirection};

use super::{helper, IndexScan, Plan, RangeQuery, SeqScan};
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
            if order.direction != SortDirection::Ascending {
                todo!("Support descending sort option");
            }
            // Find the field index.
            let (field_index, _) = get_field_index_and_type(&order.field_name, &self.schema.fields)
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
    filters: &mut Vec<IndexFilter>,
) -> Result<(), PlanError> {
    match expression {
        FilterExpression::Simple(field_name, operator, value) => {
            let (field_index, field_type) = get_field_index_and_type(field_name, &schema.fields)
                .ok_or(PlanError::FieldNotFound)?;
            let field =
                json_value_to_field(value.as_str().unwrap_or(&value.to_string()), &field_type)?;
            filters.push(IndexFilter::new(field_index, *operator, field));
        }
        FilterExpression::And(expressions) => {
            for expression in expressions {
                collect_filters(schema, expression, filters)?;
            }
        }
    }
    Ok(())
}

fn seen_in_sorted_inverted_filter(field_index: usize, filters: &[IndexFilter]) -> bool {
    filters
        .iter()
        .any(|filter| filter.field_index == field_index && filter.op.supported_by_sorted_inverted())
}

fn find_range_query(
    filters: &mut Vec<IndexFilter>,
    order_by: &[(usize, SortDirection)],
) -> Result<Option<RangeQuery>, PlanError> {
    let mut num_range_ops = 0;
    let mut range_filter_index = None;
    for (i, filter) in filters.iter().enumerate() {
        if filter.op.is_range_operator() {
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
        Some(RangeQuery {
            field_index: filter.field_index,
            operator_and_value: Some((filter.op, filter.val)),
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
                    .all(|(filter, field)| filter.field_index == field.0)
                {
                    return false;
                }
                if let Some(range_query) = range_query {
                    if fields.len() != eq_filters.len() + 1 {
                        return false;
                    }
                    range_query.field_index == fields.last().unwrap().0
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
