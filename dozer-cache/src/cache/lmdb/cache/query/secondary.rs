use std::{cmp::Ordering, ops::Bound};

use dozer_storage::lmdb::Transaction;
use dozer_types::{
    borrow::{Borrow, IntoOwned},
    types::{Field, IndexDefinition},
};

use crate::{
    cache::{
        expression::{Operator, SortDirection},
        index,
        lmdb::cache::secondary_environment::SecondaryEnvironment,
        plan::{IndexScanKind, SortedInvertedRangeQuery},
    },
    errors::{CacheError, IndexError},
};

use super::lmdb_cmp::lmdb_cmp;

pub fn build_index_scan<'txn, T: Transaction, S: SecondaryEnvironment>(
    secondary_txn: &'txn T,
    secondary_env: &S,
    index_scan_kind: &IndexScanKind,
) -> Result<impl Iterator<Item = Result<u64, CacheError>> + 'txn, CacheError> {
    let is_single_field_sorted_inverted =
        is_single_field_sorted_inverted(secondary_env.index_definition());
    let range = get_range_spec(index_scan_kind, is_single_field_sorted_inverted)?;

    let start = match &range.start {
        Some(KeyEndpoint::Including(key)) => Bound::Included(key.as_slice()),
        Some(KeyEndpoint::Excluding(key)) => Bound::Excluded(key.as_slice()),
        None => Bound::Unbounded,
    };

    let database = secondary_env.database().database();
    Ok(secondary_env
        .database()
        .range(
            secondary_txn,
            start,
            range.direction == SortDirection::Ascending,
        )?
        .take_while(move |result| match result {
            Ok((key, _)) => {
                if let Some(end_key) = &range.end {
                    match lmdb_cmp(secondary_txn, database, key.borrow(), end_key.key()) {
                        Ordering::Less => {
                            matches!(range.direction, SortDirection::Ascending)
                        }
                        Ordering::Equal => matches!(end_key, KeyEndpoint::Including(_)),
                        Ordering::Greater => {
                            matches!(range.direction, SortDirection::Descending)
                        }
                    }
                } else {
                    true
                }
            }
            Err(_) => true,
        })
        .map(|result| {
            result
                .map(|(_, id)| id.into_owned())
                .map_err(CacheError::Storage)
        }))
}

fn is_single_field_sorted_inverted(index: &IndexDefinition) -> bool {
    match index {
        // `fields.len() == 1` criteria must be kept the same with `comparator.rs`.
        IndexDefinition::SortedInverted(fields) => fields.len() == 1,
        _ => false,
    }
}

#[derive(Debug, Clone)]
pub enum KeyEndpoint {
    Including(Vec<u8>),
    Excluding(Vec<u8>),
}

impl KeyEndpoint {
    pub fn key(&self) -> &[u8] {
        match self {
            KeyEndpoint::Including(key) => key,
            KeyEndpoint::Excluding(key) => key,
        }
    }
}

#[derive(Debug)]
struct RangeSpec {
    start: Option<KeyEndpoint>,
    end: Option<KeyEndpoint>,
    direction: SortDirection,
}

fn get_range_spec(
    index_scan_kind: &IndexScanKind,
    is_single_field_sorted_inverted: bool,
) -> Result<RangeSpec, CacheError> {
    match &index_scan_kind {
        IndexScanKind::SortedInverted {
            eq_filters,
            range_query,
        } => {
            let comparison_key = build_sorted_inverted_comparison_key(
                eq_filters,
                range_query.as_ref(),
                is_single_field_sorted_inverted,
            );
            // There're 3 cases:
            // 1. Range query with operator.
            // 2. Range query without operator (only order by).
            // 3. No range query.
            Ok(if let Some(range_query) = range_query {
                match range_query.operator_and_value {
                    Some((operator, _)) => {
                        // Here we respond to case 1, examples are `a = 1 && b > 2` or `b < 2`.
                        let comparison_key = comparison_key.expect("here's at least a range query");
                        let null_key = build_sorted_inverted_comparison_key(
                            eq_filters,
                            Some(&SortedInvertedRangeQuery {
                                field_index: range_query.field_index,
                                operator_and_value: Some((operator, Field::Null)),
                                sort_direction: range_query.sort_direction,
                            }),
                            is_single_field_sorted_inverted,
                        )
                        .expect("we provided a range query");
                        get_key_interval_from_range_query(
                            comparison_key,
                            null_key,
                            operator,
                            range_query.sort_direction,
                        )
                    }
                    None => {
                        // Here we respond to case 2, examples are `a = 1 && b asc` or `b desc`.
                        if let Some(comparison_key) = comparison_key {
                            // This is the case like `a = 1 && b asc`. The comparison key is only built from `a = 1`.
                            // We use `a = 1 && b = null` as a sentinel, using the invariant that `null` is greater than anything.
                            let null_key = build_sorted_inverted_comparison_key(
                                eq_filters,
                                Some(&SortedInvertedRangeQuery {
                                    field_index: range_query.field_index,
                                    operator_and_value: Some((Operator::LT, Field::Null)),
                                    sort_direction: range_query.sort_direction,
                                }),
                                is_single_field_sorted_inverted,
                            )
                            .expect("we provided a range query");
                            match range_query.sort_direction {
                                SortDirection::Ascending => RangeSpec {
                                    start: Some(KeyEndpoint::Excluding(comparison_key)),
                                    end: Some(KeyEndpoint::Including(null_key)),
                                    direction: SortDirection::Ascending,
                                },
                                SortDirection::Descending => RangeSpec {
                                    start: Some(KeyEndpoint::Including(null_key)),
                                    end: Some(KeyEndpoint::Excluding(comparison_key)),
                                    direction: SortDirection::Descending,
                                },
                            }
                        } else {
                            // Just all of them.
                            RangeSpec {
                                start: None,
                                end: None,
                                direction: range_query.sort_direction,
                            }
                        }
                    }
                }
            } else {
                // Here we respond to case 3, examples are `a = 1` or `a = 1 && b = 2`.
                let comparison_key = comparison_key
                    .expect("here's at least a eq filter because there's no range query");
                RangeSpec {
                    start: Some(KeyEndpoint::Including(comparison_key.clone())),
                    end: Some(KeyEndpoint::Including(comparison_key)),
                    direction: SortDirection::Ascending, // doesn't matter
                }
            })
        }
        IndexScanKind::FullText { filter } => match filter.op {
            Operator::Contains => {
                let token = match &filter.val {
                    Field::String(token) => token,
                    Field::Text(token) => token,
                    _ => return Err(CacheError::Index(IndexError::ExpectedStringFullText)),
                };
                let key = index::get_full_text_secondary_index(token);
                Ok(RangeSpec {
                    start: Some(KeyEndpoint::Including(key.clone())),
                    end: Some(KeyEndpoint::Including(key)),
                    direction: SortDirection::Ascending, // doesn't matter
                })
            }
            Operator::MatchesAll | Operator::MatchesAny => {
                unimplemented!("matches all and matches any are not implemented")
            }
            other => panic!("operator {other:?} is not supported by full text index"),
        },
    }
}

fn build_sorted_inverted_comparison_key(
    eq_filters: &[(usize, Field)],
    range_query: Option<&SortedInvertedRangeQuery>,
    is_single_field_index: bool,
) -> Option<Vec<u8>> {
    let mut fields = vec![];
    eq_filters.iter().for_each(|filter| {
        fields.push(&filter.1);
    });
    if let Some(range_query) = range_query {
        if let Some((_, val)) = &range_query.operator_and_value {
            fields.push(val);
        }
    }
    if fields.is_empty() {
        None
    } else {
        Some(index::get_secondary_index(&fields, is_single_field_index))
    }
}

/// Here we use the invariant that `null` is greater than anything.
fn get_key_interval_from_range_query(
    comparison_key: Vec<u8>,
    null_key: Vec<u8>,
    operator: Operator,
    sort_direction: SortDirection,
) -> RangeSpec {
    match (operator, sort_direction) {
        (Operator::LT, SortDirection::Ascending) => RangeSpec {
            start: None,
            end: Some(KeyEndpoint::Excluding(comparison_key)),
            direction: SortDirection::Ascending,
        },
        (Operator::LT, SortDirection::Descending) => RangeSpec {
            start: Some(KeyEndpoint::Excluding(comparison_key)),
            end: None,
            direction: SortDirection::Descending,
        },
        (Operator::LTE, SortDirection::Ascending) => RangeSpec {
            start: None,
            end: Some(KeyEndpoint::Including(comparison_key)),
            direction: SortDirection::Ascending,
        },
        (Operator::LTE, SortDirection::Descending) => RangeSpec {
            start: Some(KeyEndpoint::Including(comparison_key)),
            end: None,
            direction: SortDirection::Descending,
        },
        (Operator::GT, SortDirection::Ascending) => RangeSpec {
            start: Some(KeyEndpoint::Excluding(comparison_key)),
            end: Some(KeyEndpoint::Excluding(null_key)),
            direction: SortDirection::Ascending,
        },
        (Operator::GT, SortDirection::Descending) => RangeSpec {
            start: Some(KeyEndpoint::Excluding(null_key)),
            end: Some(KeyEndpoint::Excluding(comparison_key)),
            direction: SortDirection::Descending,
        },
        (Operator::GTE, SortDirection::Ascending) => RangeSpec {
            start: Some(KeyEndpoint::Including(comparison_key)),
            end: Some(KeyEndpoint::Excluding(null_key)),
            direction: SortDirection::Ascending,
        },
        (Operator::GTE, SortDirection::Descending) => RangeSpec {
            start: Some(KeyEndpoint::Excluding(null_key)),
            end: Some(KeyEndpoint::Including(comparison_key)),
            direction: SortDirection::Descending,
        },
        (other, _) => {
            panic!("operator {other:?} is not supported by sorted inverted index range query")
        }
    }
}
