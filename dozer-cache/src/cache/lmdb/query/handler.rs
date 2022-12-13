use std::{cmp::Ordering, sync::Arc};

use super::{
    helper,
    iterator::{CacheIterator, KeyEndpoint},
};
use crate::cache::{
    expression::{Operator, QueryExpression},
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
use dozer_types::types::SortDirection;
use dozer_types::{
    bincode,
    types::{Field, IndexDefinition, Record, Schema},
};
use itertools::Either;
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
        CacheIterator::new(cursor, None, true)
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

        let range_spec =
            get_range_spec(&index_scan.kind, index_scan.is_single_field_sorted_inverted)?;

        let cursor = self
            .txn
            .open_ro_cursor(index_db)
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;

        Ok(match range_spec {
            RangeSpec::KeyInterval { start, end } => Either::Left(
                CacheIterator::new(cursor, start, true)
                    .take_while(move |(key, _)| {
                        if let Some(end_key) = &end {
                            match lmdb_cmp(self.txn, index_db, key, end_key.key()) {
                                Ordering::Less => true,
                                Ordering::Equal => matches!(end_key, KeyEndpoint::Including(_)),
                                Ordering::Greater => false,
                            }
                        } else {
                            true
                        }
                    })
                    .map(|(_, id)| id),
            ),
            RangeSpec::KeyPrefix(prefix) => Either::Right(
                CacheIterator::new(cursor, Some(KeyEndpoint::Excluding(prefix.clone())), true)
                    .take_while(move |(key, _)| key.starts_with(&prefix))
                    .map(|(_, id)| id),
            ),
        })
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
enum RangeSpec {
    KeyInterval {
        start: Option<KeyEndpoint>,
        end: Option<KeyEndpoint>,
    },
    KeyPrefix(Vec<u8>),
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
                        let (start, end) = get_key_interval_from_range_query(
                            comparison_key,
                            operator,
                            range_query.sort_direction,
                        );
                        RangeSpec::KeyInterval { start, end }
                    }
                    None => {
                        // Here we respond to case 2, examples are `a = 1 && b asc` or `b desc`.
                        if let Some(comparison_key) = comparison_key {
                            RangeSpec::KeyPrefix(comparison_key)
                        } else {
                            // Just all of them.
                            RangeSpec::KeyInterval {
                                start: None,
                                end: None,
                            }
                        }
                    }
                }
            } else {
                // Here we respond to case 3, examples are `a = 1` or `a = 1 && b = 2`.
                let comparison_key = comparison_key
                    .expect("here's at least a eq filter because there's no range query");
                RangeSpec::KeyInterval {
                    start: Some(KeyEndpoint::Including(comparison_key.clone())),
                    end: Some(KeyEndpoint::Including(comparison_key)),
                }
            })
        }
        IndexScanKind::FullText { filter } => match filter.op {
            Operator::Contains => {
                if let Field::String(token) = &filter.val {
                    let key = index::get_full_text_secondary_index(token);
                    Ok(RangeSpec::KeyInterval {
                        start: Some(KeyEndpoint::Including(key.clone())),
                        end: Some(KeyEndpoint::Including(key)),
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
    eq_filters: &[(usize, SortDirection, Field)],
    range_query: Option<&SortedInvertedRangeQuery>,
    is_single_field_index: bool,
) -> Option<Vec<u8>> {
    let mut fields = vec![];
    eq_filters.iter().for_each(|filter| {
        fields.push((&filter.2, filter.1));
    });
    if let Some(range_query) = range_query {
        if let Some((_, val)) = &range_query.operator_and_value {
            fields.push((val, range_query.sort_direction));
        }
    }
    if fields.is_empty() {
        None
    } else {
        Some(index::get_secondary_index(&fields, is_single_field_index))
    }
}

fn get_key_interval_from_range_query(
    comparison_key: Vec<u8>,
    operator: Operator,
    sort_direction: SortDirection,
) -> (Option<KeyEndpoint>, Option<KeyEndpoint>) {
    match (operator, sort_direction) {
        (Operator::LT, SortDirection::Ascending) => {
            (None, Some(KeyEndpoint::Excluding(comparison_key)))
        }
        (Operator::LT, SortDirection::Descending) => {
            (Some(KeyEndpoint::Excluding(comparison_key)), None)
        }
        (Operator::LTE, SortDirection::Ascending) => {
            (None, Some(KeyEndpoint::Including(comparison_key)))
        }
        (Operator::LTE, SortDirection::Descending) => {
            (Some(KeyEndpoint::Including(comparison_key)), None)
        }
        (Operator::GT, SortDirection::Ascending) => {
            (Some(KeyEndpoint::Excluding(comparison_key)), None)
        }
        (Operator::GT, SortDirection::Descending) => {
            (None, Some(KeyEndpoint::Excluding(comparison_key)))
        }
        (Operator::GTE, SortDirection::Ascending) => {
            (Some(KeyEndpoint::Including(comparison_key)), None)
        }
        (Operator::GTE, SortDirection::Descending) => {
            (None, Some(KeyEndpoint::Including(comparison_key)))
        }
        (other, _) => panic!(
            "operator {:?} is not supported by sorted inverted index range query",
            other
        ),
    }
}
