use std::sync::Arc;

use super::{helper, iterator::CacheIterator};
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
use dozer_types::{
    bincode,
    types::{Field, Record, Schema},
};
use dozer_types::{errors::types::TypeError, types::SortDirection};
use lmdb::{Database, RoTransaction, Transaction};

pub struct LmdbQueryHandler<'a> {
    db: &'a Database,
    index_metadata: Arc<IndexMetaData>,
    txn: &'a RoTransaction<'a>,
    schema: &'a Schema,
    query: &'a QueryExpression,
}
impl<'a> LmdbQueryHandler<'a> {
    pub fn new(
        db: &'a Database,
        index_metadata: Arc<IndexMetaData>,
        txn: &'a RoTransaction,
        schema: &'a Schema,
        query: &'a QueryExpression,
    ) -> Self {
        Self {
            db,
            index_metadata,
            txn,
            schema,
            query,
        }
    }

    pub fn query(&self) -> Result<Vec<Record>, CacheError> {
        let planner = QueryPlanner::new(self.schema, self.query);
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
        let mut cache_iterator = CacheIterator::new(&cursor, None, true);
        // cache_iterator.skip(skip);

        let mut records = vec![];
        let mut idx = 0;
        loop {
            let rec = cache_iterator.next();
            if self.query.skip > idx {
                //
            } else if rec.is_some() && idx < self.query.limit {
                if let Some((_key, val)) = rec {
                    let rec = bincode::deserialize::<Record>(val).map_err(|e| {
                        TypeError::SerializationError(
                            dozer_types::errors::types::SerializationError::Bincode(e),
                        )
                    })?;
                    records.push(rec);
                } else {
                    break;
                }
            } else {
                break;
            }
            idx += 1;
        }
        Ok(records)
    }

    fn query_with_secondary_index(
        &self,
        index_scans: &[IndexScan],
    ) -> Result<Vec<Record>, CacheError> {
        let index_scan = index_scans[0].to_owned();
        let db = self.index_metadata.get_db(self.schema, index_scan.index_id);

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

        let mut pkeys = vec![];
        let mut idx = 0;

        let cursor = self
            .txn
            .open_ro_cursor(db)
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;

        let mut cache_iterator =
            CacheIterator::new(&cursor, start_key.as_ref().map(|a| a as &[u8]), true);
        // For GT, LT operators dont include the first record returned.

        loop {
            let tuple = cache_iterator.next();

            if self.query.skip > idx {
            } else if idx < self.query.limit {
                // Check if the tuple returns a value
                if let Some((key, val)) = tuple {
                    // Skip Eq Values
                    if self.skip_eq_values(
                        &db,
                        last_filter,
                        start_key.as_ref(),
                        end_key.as_ref(),
                        key,
                    ) {
                    }
                    // Compare partial key
                    else if self.compare_key(
                        &db,
                        key,
                        start_key.as_ref(),
                        end_key.as_ref(),
                        last_filter,
                    ) {
                        let rec = helper::get(self.txn, *self.db, val)?;
                        pkeys.push(rec);
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
            idx += 1;
        }
        Ok(pkeys)
    }

    // Based on the filters provided and sort_order, determine to include the first result.
    fn skip_eq_values(
        &self,
        db: &Database,
        last_filter: Option<(Operator, SortDirection)>,
        start_key: Option<&Vec<u8>>,
        end_key: Option<&Vec<u8>>,
        current_key: &[u8],
    ) -> bool {
        last_filter.map_or(false, |(operator, sort_direction)| match operator {
            Operator::LT => match sort_direction {
                SortDirection::Ascending => {
                    let cmp = lmdb_cmp(self.txn, db, current_key, end_key);
                    cmp == 0
                }
                SortDirection::Descending => {
                    let end_cmp = lmdb_cmp(self.txn, db, current_key, end_key);
                    end_cmp == 0
                }
            },
            Operator::GT => match sort_direction {
                SortDirection::Ascending => {
                    let cmp = lmdb_cmp(self.txn, db, current_key, start_key);
                    cmp == 0
                }
                SortDirection::Descending => {
                    let end_cmp = lmdb_cmp(self.txn, db, current_key, end_key);
                    end_cmp == 0
                }
            },
            Operator::GTE
            | Operator::LTE
            | Operator::EQ
            | Operator::Contains
            | Operator::MatchesAny
            | Operator::MatchesAll => false,
        })
    }

    fn compare_key(
        &self,
        db: &Database,
        key: &[u8],
        start_key: Option<&Vec<u8>>,
        end_key: Option<&Vec<u8>>,
        last_filter: Option<(Operator, SortDirection)>,
    ) -> bool {
        let cmp = lmdb_cmp(self.txn, db, key, start_key);
        let end_cmp = lmdb_cmp(self.txn, db, key, end_key);

        last_filter.map_or(cmp == 0, |(operator, sort_direction)| match operator {
            Operator::LT => match sort_direction {
                SortDirection::Ascending => end_cmp < 0,
                SortDirection::Descending => cmp >= 0,
            },
            Operator::LTE => match sort_direction {
                SortDirection::Ascending => end_cmp <= 0,
                SortDirection::Descending => cmp > 0,
            },

            Operator::GT => match sort_direction {
                SortDirection::Ascending => cmp > 0,
                SortDirection::Descending => end_cmp <= 0,
            },
            Operator::GTE => match sort_direction {
                SortDirection::Ascending => cmp >= 0,
                SortDirection::Descending => end_cmp < 0,
            },
            Operator::EQ | Operator::Contains | Operator::MatchesAny | Operator::MatchesAll => {
                cmp == 0
            }
        })
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
                index::get_secondary_index(&fields).map_err(CacheError::map_serialization_error)
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
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    last_filter.map_or(
        (Some(comp_key.to_owned()), None),
        |(operator, sort_direction)| match operator {
            Operator::LT | Operator::LTE => match sort_direction {
                SortDirection::Ascending => (None, Some(comp_key)),
                SortDirection::Descending => (Some(comp_key), None),
            },

            Operator::GT | Operator::GTE => match sort_direction {
                SortDirection::Ascending => (Some(comp_key), None),
                SortDirection::Descending => (None, Some(comp_key)),
            },
            Operator::EQ | Operator::Contains | Operator::MatchesAny | Operator::MatchesAll => {
                (Some(comp_key), None)
            }
        },
    )
}
