use itertools::Itertools;

use dozer_types::types::{Field, IndexDefinition, SortDirection};

use crate::cache::expression::Operator;

use super::{IndexFilter, IndexScan};

#[derive(Debug, Clone, PartialEq)]
pub struct RangeQuery {
    pub field_index: usize,
    pub operator_and_value: Option<(Operator, Field)>,
}

pub fn get_all_indexes(
    filters: Vec<(usize, Operator, Field)>,
    range_query: Option<RangeQuery>,
) -> impl Iterator<Item = Vec<IndexScan>> {
    // Create a full text index for every full text filter, and collect `Eq` filters.
    let mut full_text_scans = vec![];
    let mut eq_filters = vec![];
    for filter in filters {
        if filter.1.supported_by_full_text() {
            full_text_scans.push(IndexScan {
                index_def: IndexDefinition::FullText(filter.0),
                // filters: vec![Some(filter.2)],
                filters: vec![Some(IndexFilter::new(filter.1, filter.2))],
                index_id: None,
            });
        } else {
            debug_assert!(filter.1 == Operator::EQ);
            eq_filters.push((filter.0, filter.2));
        }
    }

    // The `Eq` filters can be of arbitary order.
    let num_eq_filters = eq_filters.len();
    eq_filters
        .into_iter()
        .permutations(num_eq_filters)
        .map(move |filters| {
            // Collect `Eq` field indexes and values.
            let mut fields = filters
                .iter()
                .map(|(index, _)| (*index, SortDirection::Ascending))
                .collect::<Vec<_>>();
            let mut values = filters
                .into_iter()
                .map(|(_, value)| Some(IndexFilter::equals(value)))
                .collect::<Vec<_>>();
            // Append range query if necessary.
            if let Some(RangeQuery {
                field_index,
                operator_and_value,
            }) = range_query.clone()
            {
                fields.push((field_index, SortDirection::Ascending));
                values.push(operator_and_value.map(|(op, val)| IndexFilter::new(op, val)));
            }

            let mut index_scans = full_text_scans.clone();
            // Append sorted inverted index scan if necessary.
            if !fields.is_empty() {
                index_scans.push(IndexScan {
                    index_def: IndexDefinition::SortedInverted(fields),
                    filters: values,
                    index_id: None,
                });
            }
            index_scans
        })
}
