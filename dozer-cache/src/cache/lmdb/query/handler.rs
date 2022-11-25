use std::{cmp::Ordering, sync::Arc};

use super::{
    helper,
    iterator::{CacheIterator, KeyEndpoint},
};
use crate::cache::{
    expression::{Operator, QueryExpression},
    index::{self},
    lmdb::{cache::IndexMetaData, query::helper::lmdb_cmp},
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
use lmdb::{Database, RoTransaction, Transaction};

pub struct LmdbQueryHandler<'a> {
    db: &'a Database,
    index_metadata: Arc<IndexMetaData>,
    txn: &'a RoTransaction<'a>,
    schema: &'a Schema,
    secondary_indexes: &'a [IndexDefinition],
    query: &'a QueryExpression,
}
impl<'a> LmdbQueryHandler<'a> {
    pub fn new(
        db: &'a Database,
        index_metadata: Arc<IndexMetaData>,
        txn: &'a RoTransaction,
        schema: &'a Schema,
        secondary_indexes: &'a [IndexDefinition],
        query: &'a QueryExpression,
    ) -> Self {
        Self {
            db,
            index_metadata,
            txn,
            schema,
            secondary_indexes,
            query,
        }
    }

    pub fn query(&self) -> Result<Vec<Record>, CacheError> {
        let planner = QueryPlanner::new(self.schema, self.secondary_indexes, self.query);
        let execution = planner.plan()?;
        let records = match execution {
            Plan::IndexScans(index_scans) => {
                if index_scans.len() > 1 {
                    todo!("Combine results from multiple index scans");
                }
                debug_assert!(
                    !index_scans.is_empty(),
                    "Planner should not generate empty index scan"
                );
                self.query_with_secondary_index(&index_scans)?
            }
            Plan::SeqScan(_seq_scan) => self.iterate_and_deserialize()?,
        };

        Ok(records)
    }

    pub fn iterate_and_deserialize(&self) -> Result<Vec<Record>, CacheError> {
        let cursor = self
            .txn
            .open_ro_cursor(*self.db)
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        CacheIterator::new(cursor, None, true)
            .skip(self.query.skip)
            .take(self.query.limit)
            .map(|(_, v)| bincode::deserialize(v).map_err(CacheError::map_deserialization_error))
            .collect::<Result<Vec<_>, CacheError>>()
    }

    fn query_with_secondary_index(
        &self,
        index_scans: &[IndexScan],
    ) -> Result<Vec<Record>, CacheError> {
        let index_scan = index_scans[0].to_owned();
        let index_db = self.index_metadata.get_db(self.schema, index_scan.index_id);

        let comparision_key = self.build_comparision_key(&index_scan)?;
        let last_filter = match index_scan.kind {
            IndexScanKind::SortedInverted {
                range_query:
                    Some(SortedInvertedRangeQuery {
                        sort_direction,
                        operator_and_value: Some((operator, _)),
                        ..
                    }),
                ..
            } => Some((operator, sort_direction)),
            IndexScanKind::SortedInverted { eq_filters, .. } => {
                let filter = eq_filters.last().unwrap();
                Some((Operator::EQ, filter.1))
            }
            IndexScanKind::FullText { filter } => Some((filter.op, SortDirection::Ascending)),
        };

        let (start_key, end_key) = get_start_end_keys(last_filter, comparision_key);

        let cursor = self
            .txn
            .open_ro_cursor(index_db)
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;

        CacheIterator::new(cursor, start_key, true)
            .skip(self.query.skip)
            .take(self.query.limit)
            .take_while(move |(key, _)| {
                if let Some(end_key) = &end_key {
                    match lmdb_cmp(self.txn, index_db, key, end_key.key()) {
                        Ordering::Less => true,
                        Ordering::Equal => matches!(end_key, KeyEndpoint::Including(_)),
                        Ordering::Greater => false,
                    }
                } else {
                    true
                }
            })
            .map(|(_, id)| helper::get(self.txn, *self.db, id))
            .collect::<Result<Vec<_>, CacheError>>()
    }

    fn build_comparision_key(&self, index_scan: &'a IndexScan) -> Result<Vec<u8>, CacheError> {
        match &index_scan.kind {
            IndexScanKind::SortedInverted {
                eq_filters,
                range_query,
            } => {
                let mut fields = vec![];
                eq_filters.iter().for_each(|filter| {
                    fields.push((&filter.2, filter.1));
                });
                if let Some(range_query) = range_query {
                    if let Some((_, val)) = &range_query.operator_and_value {
                        fields.push((val, range_query.sort_direction));
                    }
                }
                index::get_secondary_index(&fields)
            }
            IndexScanKind::FullText { filter } => {
                if let Field::String(token) = &filter.val {
                    Ok(index::get_full_text_secondary_index(token))
                } else {
                    Err(CacheError::IndexError(IndexError::ExpectedStringFullText))
                }
            }
        }
    }
}

fn get_start_end_keys(
    last_filter: Option<(Operator, SortDirection)>,
    comp_key: Vec<u8>,
) -> (Option<KeyEndpoint>, Option<KeyEndpoint>) {
    last_filter.map_or(
        (
            Some(KeyEndpoint::Including(comp_key.clone())),
            Some(KeyEndpoint::Including(comp_key.clone())),
        ),
        |(operator, sort_direction)| match (operator, sort_direction) {
            (Operator::LT, SortDirection::Ascending) => {
                (None, Some(KeyEndpoint::Excluding(comp_key)))
            }
            (Operator::LT, SortDirection::Descending) => {
                (Some(KeyEndpoint::Excluding(comp_key)), None)
            }
            (Operator::LTE, SortDirection::Ascending) => {
                (None, Some(KeyEndpoint::Including(comp_key)))
            }
            (Operator::LTE, SortDirection::Descending) => {
                (Some(KeyEndpoint::Including(comp_key)), None)
            }
            (Operator::GT, SortDirection::Ascending) => {
                (Some(KeyEndpoint::Excluding(comp_key)), None)
            }
            (Operator::GT, SortDirection::Descending) => {
                (None, Some(KeyEndpoint::Excluding(comp_key)))
            }
            (Operator::GTE, SortDirection::Ascending) => {
                (Some(KeyEndpoint::Including(comp_key)), None)
            }
            (Operator::GTE, SortDirection::Descending) => {
                (None, Some(KeyEndpoint::Including(comp_key)))
            }
            (
                Operator::EQ | Operator::Contains | Operator::MatchesAny | Operator::MatchesAll,
                _,
            ) => (
                Some(KeyEndpoint::Including(comp_key.clone())),
                Some(KeyEndpoint::Including(comp_key)),
            ),
        },
    )
}
