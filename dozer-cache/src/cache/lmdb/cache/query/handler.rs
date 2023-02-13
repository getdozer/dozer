use std::cmp::Ordering;

use super::intersection::intersection;
use super::iterator::{CacheIterator, KeyEndpoint};
use crate::cache::lmdb::cache::{id_from_bytes, id_to_bytes, LmdbCacheCommon};
use crate::cache::RecordWithId;
use crate::cache::{
    expression::{Operator, QueryExpression, SortDirection},
    index,
    plan::{IndexScan, IndexScanKind, Plan, QueryPlanner, SortedInvertedRangeQuery},
};
use crate::errors::{CacheError, IndexError};
use dozer_storage::lmdb::Transaction;
use dozer_types::types::{Field, IndexDefinition, Schema};
use itertools::Either;

pub struct LmdbQueryHandler<'a, T: Transaction> {
    common: &'a LmdbCacheCommon,
    txn: &'a T,
    schema: Schema,
    secondary_indexes: Vec<IndexDefinition>,
    query: &'a QueryExpression,
}
impl<'a, T: Transaction> LmdbQueryHandler<'a, T> {
    pub fn new(
        common: &'a LmdbCacheCommon,
        txn: &'a T,
        schema: Schema,
        secondary_indexes: Vec<IndexDefinition>,
        query: &'a QueryExpression,
    ) -> Self {
        Self {
            common,
            txn,
            schema,
            secondary_indexes,
            query,
        }
    }

    pub fn count(&self) -> Result<usize, CacheError> {
        let planner = QueryPlanner::new(&self.schema, &self.secondary_indexes, self.query);
        let execution = planner.plan()?;
        match execution {
            Plan::IndexScans(index_scans) => Ok(self.build_index_scan(index_scans)?.count()),
            Plan::SeqScan(_) => Ok(self
                .common
                .db
                .count(self.txn)?
                .saturating_sub(self.query.skip)
                .min(self.query.limit.unwrap_or(usize::MAX))),
            Plan::ReturnEmpty => Ok(0),
        }
    }

    pub fn query(&self) -> Result<Vec<RecordWithId>, CacheError> {
        let planner = QueryPlanner::new(&self.schema, &self.secondary_indexes, self.query);
        let execution = planner.plan()?;
        match execution {
            Plan::IndexScans(index_scans) => {
                self.collect_records(self.build_index_scan(index_scans)?)
            }
            Plan::SeqScan(_seq_scan) => self.collect_records(self.all_ids()?),
            Plan::ReturnEmpty => Ok(vec![]),
        }
    }

    pub fn all_ids(&self) -> Result<impl Iterator<Item = [u8; 8]> + '_, CacheError> {
        let cursor = self.common.id.open_ro_cursor(self.txn)?;
        Ok(CacheIterator::new(cursor, None, SortDirection::Ascending)
            .skip(self.query.skip)
            .take(self.query.limit.unwrap_or(usize::MAX))
            .map(|(_, value)| {
                value
                    .try_into()
                    .expect("All values must be u64 ids in id database")
            }))
    }

    fn build_index_scan(
        &self,
        index_scans: Vec<IndexScan>,
    ) -> Result<impl Iterator<Item = [u8; 8]> + '_, CacheError> {
        debug_assert!(
            !index_scans.is_empty(),
            "Planner should not generate empty index scan"
        );
        let full_scan = if index_scans.len() == 1 {
            // The fast path, without intersection calculation.
            Either::Left(self.query_with_secondary_index(&index_scans[0])?)
        } else {
            // Intersection of multiple index scans.
            let iterators = index_scans
                .iter()
                .map(|index_scan| {
                    self.query_with_secondary_index(index_scan)
                        .map(|iter| iter.map(id_from_bytes))
                })
                .collect::<Result<Vec<_>, CacheError>>()?;
            Either::Right(
                intersection(iterators, self.common.cache_options.intersection_chunk_size)
                    .map(id_to_bytes),
            )
        };
        Ok(full_scan
            .skip(self.query.skip)
            .take(self.query.limit.unwrap_or(usize::MAX)))
    }

    fn query_with_secondary_index(
        &'a self,
        index_scan: &IndexScan,
    ) -> Result<impl Iterator<Item = [u8; 8]> + 'a, CacheError> {
        let schema_id = self
            .schema
            .identifier
            .ok_or(CacheError::SchemaIdentifierNotFound)?;
        let index_db = *self
            .common
            .secondary_indexes
            .read()
            .get(&(schema_id, index_scan.index_id))
            .ok_or(CacheError::SecondaryIndexDatabaseNotFound)?;

        let RangeSpec {
            start,
            end,
            direction,
        } = get_range_spec(&index_scan.kind, index_scan.is_single_field_sorted_inverted)?;

        let cursor = index_db.open_ro_cursor(self.txn)?;

        Ok(CacheIterator::new(cursor, start, direction)
            .take_while(move |(key, _)| {
                if let Some(end_key) = &end {
                    match index_db.cmp(self.txn, key, end_key.key()) {
                        Ordering::Less => matches!(direction, SortDirection::Ascending),
                        Ordering::Equal => matches!(end_key, KeyEndpoint::Including(_)),
                        Ordering::Greater => matches!(direction, SortDirection::Descending),
                    }
                } else {
                    true
                }
            })
            .map(|(_, id)| {
                id.try_into()
                    .expect("All values must be u64 ids in secondary index database")
            }))
    }

    fn collect_records(
        &self,
        ids: impl Iterator<Item = [u8; 8]>,
    ) -> Result<Vec<RecordWithId>, CacheError> {
        ids.map(|id| {
            self.common
                .db
                .get(self.txn, id)
                .map(|record| RecordWithId::new(id_from_bytes(id), record))
        })
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
        (other, _) => {
            panic!("operator {other:?} is not supported by sorted inverted index range query")
        }
    }
}
