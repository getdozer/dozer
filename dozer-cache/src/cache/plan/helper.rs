use dozer_types::types::Field;
use itertools::{Either, Itertools};

use crate::cache::expression::{Operator, SortDirection};

use super::{IndexFilter, IndexScanKind, SortedInvertedRangeQuery};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeQuery {
    pub field_index: usize,
    pub kind: RangeQueryKind,
}

impl RangeQuery {
    pub fn new(field_index: usize, kind: RangeQueryKind) -> Self {
        Self { field_index, kind }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeQueryKind {
    Filter {
        operator: Operator,
        value: Field,
        sort_direction: Option<SortDirection>,
    },
    OrderBy {
        sort_direction: SortDirection,
    },
}

pub fn get_all_indexes(
    filters: Vec<(IndexFilter, Option<SortDirection>)>,
    range_query: Option<RangeQuery>,
) -> impl Iterator<Item = Vec<IndexScanKind>> {
    // Create a full text index for every full text filter, and collect `Eq` filters.
    let mut full_text_scans = vec![];
    let mut eq_filters = vec![];
    for filter in filters {
        if filter.0.op.supported_by_full_text() {
            full_text_scans.push(IndexScanKind::FullText { filter: filter.0 });
        } else {
            debug_assert!(filter.0.op == Operator::EQ);
            eq_filters.push((filter.0.field_index, filter.0.val));
        }
    }

    if eq_filters.is_empty() && range_query.is_none() {
        // Only full text scans.
        assert!(
            !full_text_scans.is_empty(),
            "Must have at least one filter or range query"
        );
        Either::Left(std::iter::once(full_text_scans))
    } else {
        Either::Right(
            get_sorted_inverted_scans(eq_filters, range_query).map(move |scan| {
                let mut scans = full_text_scans.clone();
                scans.push(scan);
                scans
            }),
        )
    }
}

fn get_sorted_inverted_scans(
    eq_filters: Vec<(usize, Field)>,
    range_query: Option<RangeQuery>,
) -> impl Iterator<Item = IndexScanKind> {
    if eq_filters.is_empty() {
        Either::Left(
            get_sorted_inverted_range_queries(
                range_query.expect("Range query must not be None if eq_filters is empty"),
            )
            .map(|range_query| IndexScanKind::SortedInverted {
                eq_filters: vec![],
                range_query: Some(range_query),
            }),
        )
    } else {
        Either::Right(get_sorted_inverted_scans_with_eq_filters(
            eq_filters,
            range_query,
        ))
    }
}

fn get_sorted_inverted_scans_with_eq_filters(
    eq_filters: Vec<(usize, Field)>,
    range_query: Option<RangeQuery>,
) -> impl Iterator<Item = IndexScanKind> {
    // The `Eq` filters can be of arbitary order.
    let num_eq_filters = eq_filters.len();
    eq_filters
        .into_iter()
        .permutations(num_eq_filters)
        .flat_map(move |eq_filters| {
            get_option_sorted_inverted_range_queries(range_query.clone()).map(move |range_query| {
                IndexScanKind::SortedInverted {
                    eq_filters: eq_filters.clone(),
                    range_query,
                }
            })
        })
}

fn get_option_sorted_inverted_range_queries(
    range_query: Option<RangeQuery>,
) -> impl Iterator<Item = Option<SortedInvertedRangeQuery>> {
    if let Some(range_query) = range_query {
        Either::Left(get_sorted_inverted_range_queries(range_query).map(Some))
    } else {
        Either::Right(std::iter::once(None))
    }
}

fn get_sorted_inverted_range_queries(
    range_query: RangeQuery,
) -> impl Iterator<Item = SortedInvertedRangeQuery> {
    match range_query.kind {
        RangeQueryKind::Filter {
            operator,
            value,
            sort_direction,
        } => Either::Left(
            get_sort_directions(sort_direction).map(move |sort_direction| {
                SortedInvertedRangeQuery {
                    field_index: range_query.field_index,
                    operator_and_value: Some((operator, value.clone())),
                    sort_direction,
                }
            }),
        ),
        RangeQueryKind::OrderBy { sort_direction } => {
            Either::Right(std::iter::once(SortedInvertedRangeQuery {
                field_index: range_query.field_index,
                operator_and_value: None,
                sort_direction,
            }))
        }
    }
}

fn get_sort_directions(
    sort_direction: Option<SortDirection>,
) -> impl Iterator<Item = SortDirection> + Clone {
    if let Some(direction) = sort_direction {
        Either::Left(std::iter::once(direction))
    } else {
        Either::Right(
            std::iter::once(SortDirection::Ascending)
                .chain(std::iter::once(SortDirection::Descending)),
        )
    }
}

#[test]
#[should_panic]
fn get_all_indexes_from_empty_query_should_panic() {
    get_all_indexes(vec![], None).collect_vec();
}

#[test]
fn test_get_all_indexes() {
    fn check(
        filters: Vec<(IndexFilter, Option<SortDirection>)>,
        range_query: Option<RangeQuery>,
        expcected: Vec<Vec<IndexScanKind>>,
    ) {
        let actual = get_all_indexes(filters, range_query).collect::<Vec<_>>();
        assert_eq!(actual, expcected);
    }

    // Only full text.
    let filter = IndexFilter::new(0, Operator::Contains, Field::String("a".into()));
    check(
        vec![(filter.clone(), None)],
        None,
        vec![vec![IndexScanKind::FullText { filter }]],
    );

    // Only `Eq`.
    let filter = IndexFilter::new(0, Operator::EQ, Field::String("a".into()));
    check(
        vec![(filter.clone(), None)],
        None,
        vec![vec![IndexScanKind::SortedInverted {
            eq_filters: vec![(filter.field_index, filter.val)],
            range_query: None,
        }]],
    );

    // Only order by.
    let direction = SortDirection::Ascending;
    let range_query = RangeQuery::new(
        0,
        RangeQueryKind::OrderBy {
            sort_direction: direction,
        },
    );
    check(
        vec![],
        Some(range_query.clone()),
        vec![vec![IndexScanKind::SortedInverted {
            eq_filters: vec![],
            range_query: Some(SortedInvertedRangeQuery {
                field_index: range_query.field_index,
                operator_and_value: None,
                sort_direction: direction,
            }),
        }]],
    );
}
