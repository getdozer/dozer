use std::{cmp::Ordering, sync::Arc};

use super::{
    helper,
    iterator::{CacheIterator, KeyEndpoint},
};
use crate::cache::{
    expression::{Operator, QueryExpression, SortDirection},
    index::{self},
    lmdb::{
        cache::IndexMetaData,
        query::{helper::lmdb_cmp, intersection::intersection},
    },
    plan::{IndexScan, IndexScanKind, Plan, QueryPlanner, SortedInvertedRangeQuery},
};
use crate::errors::{
    CacheError::{self},
    IndexError,
};
use dozer_types::{
    bincode,
    types::{Field, IndexDefinition, Record, Schema},
};
use lmdb::{Database, RoTransaction, Transaction};

pub struct LmdbQueryHandler<'a> {
    db: Database,
    index_metadata: Arc<IndexMetaData>,
    txn: &'a RoTransaction<'a>,
    schema: &'a Schema,
    secondary_indexes: &'a [IndexDefinition],
    query: &'a QueryExpression,
    intersection_chunk_size: usize,
}
impl<'a> LmdbQueryHandler<'a> {
    pub fn new(
        db: Database,
        index_metadata: Arc<IndexMetaData>,
        txn: &'a RoTransaction,
        schema: &'a Schema,
        secondary_indexes: &'a [IndexDefinition],
        query: &'a QueryExpression,
        intersection_chunk_size: usize,
    ) -> Self {
        Self {
            db,
            index_metadata,
            txn,
            schema,
            secondary_indexes,
            query,
            intersection_chunk_size,
        }
    }

    pub fn query(&self) -> Result<Vec<Record>, CacheError> {
        let planner = QueryPlanner::new(self.schema, self.secondary_indexes, self.query);
        let execution = planner.plan()?;
        match execution {
            Plan::IndexScans(index_scans) => {
                debug_assert!(
                    !index_scans.is_empty(),
                    "Planner should not generate empty index scan"
                );
                if index_scans.len() == 1 {
                    // The fast path, without intersection calculation.
                    let iter = self.query_with_secondary_index(&index_scans[0])?;
                    self.collect_records(iter)
                } else {
                    // Intersection of multiple index scans.
                    let iterators = index_scans
                        .iter()
                        .map(|index_scan| {
                            self.query_with_secondary_index(index_scan).map(|iter| {
                                iter.map(|id| u64::from_be_bytes(id.try_into().unwrap()))
                            })
                        })
                        .collect::<Result<Vec<_>, CacheError>>()?;
                    let intersection = intersection(iterators, self.intersection_chunk_size);
                    self.collect_records(intersection.map(|id| id.to_be_bytes()))
                }
            }
            Plan::SeqScan(_seq_scan) => self.iterate_and_deserialize(),
            Plan::ReturnEmpty => Ok(vec![]),
        }
    }

    pub fn iterate_and_deserialize(&self) -> Result<Vec<Record>, CacheError> {
        let cursor = self
            .txn
            .open_ro_cursor(self.db)
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        CacheIterator::new(cursor, None, SortDirection::Ascending)
            .skip(self.query.skip)
            .take(self.query.limit)
            .map(|(_, v)| bincode::deserialize(v).map_err(CacheError::map_deserialization_error))
            .collect()
    }

    fn query_with_secondary_index(
        &'a self,
        index_scan: &IndexScan,
    ) -> Result<impl Iterator<Item = &'a [u8]> + 'a, CacheError> {
        let index_db = self.index_metadata.get_db(self.schema, index_scan.index_id);

        let RangeSpec {
            start,
            end,
            direction,
        } = get_range_spec(&index_scan.kind, index_scan.is_single_field_sorted_inverted)?;

        let cursor = self
            .txn
            .open_ro_cursor(index_db)
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;

        Ok(CacheIterator::new(cursor, start, direction)
            .take_while(move |(key, _)| {
                if let Some(end_key) = &end {
                    match lmdb_cmp(self.txn, index_db, key, end_key.key()) {
                        Ordering::Less => matches!(direction, SortDirection::Ascending),
                        Ordering::Equal => matches!(end_key, KeyEndpoint::Including(_)),
                        Ordering::Greater => matches!(direction, SortDirection::Descending),
                    }
                } else {
                    true
                }
            })
            .map(|(_, id)| id))
    }

    fn collect_records(
        &self,
        ids: impl Iterator<Item = impl AsRef<[u8]>>,
    ) -> Result<Vec<Record>, CacheError> {
        ids.skip(self.query.skip)
            .take(self.query.limit)
            .map(|id| helper::get(self.txn, self.db, id.as_ref()))
            .collect()
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
            let comparison_key = build_sorted_inverted_comparision_key(
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
                        let null_key = build_sorted_inverted_comparision_key(
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
                            let null_key = build_sorted_inverted_comparision_key(
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
                if let Field::String(token) = &filter.val {
                    let key = index::get_full_text_secondary_index(token);
                    Ok(RangeSpec {
                        start: Some(KeyEndpoint::Including(key.clone())),
                        end: Some(KeyEndpoint::Including(key)),
                        direction: SortDirection::Ascending, // doesn't matter
                    })
                } else {
                    Err(CacheError::IndexError(IndexError::ExpectedStringFullText))
                }
            }
            Operator::MatchesAll | Operator::MatchesAny => {
                unimplemented!("matches all and matches any are not implemented")
            }
            other => panic!("operator {:?} is not supported by full text index", other),
        },
    }
}

fn build_sorted_inverted_comparision_key(
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
        (other, _) => panic!(
            "operator {:?} is not supported by sorted inverted index range query",
            other
        ),
    }
}
