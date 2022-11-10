use itertools::Itertools;

use crate::cache::expression::Operator;

use super::{IndexFilter, IndexScanKind, RangeQuery};

pub fn get_all_indexes(
    filters: Vec<IndexFilter>,
    range_query: Option<RangeQuery>,
) -> impl Iterator<Item = Vec<IndexScanKind>> {
    // Create a full text index for every full text filter, and collect `Eq` filters.
    let mut full_text_scans = vec![];
    let mut eq_filters = vec![];
    for filter in filters {
        if filter.op.supported_by_full_text() {
            full_text_scans.push(IndexScanKind::FullText { filter });
        } else {
            debug_assert!(filter.op == Operator::EQ);
            eq_filters.push(filter);
        }
    }

    // The `Eq` filters can be of arbitary order.
    let num_eq_filters = eq_filters.len();
    eq_filters
        .into_iter()
        .permutations(num_eq_filters)
        .map(move |filters| {
            let mut index_scans = full_text_scans.clone();
            // Append sorted inverted index scan if necessary.
            if !filters.is_empty() || range_query.is_some() {
                index_scans.push(IndexScanKind::SortedInverted {
                    eq_filters: filters,
                    range_query: range_query.clone(),
                });
            }
            index_scans
        })
}
