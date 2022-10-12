use anyhow::Context;
use lmdb::{Database, RoTransaction, Transaction};

use super::{helper, iterator::CacheIterator};
use crate::cache::{
    expression::{ExecutionStep, IndexScan, QueryExpression},
    index,
    planner::QueryPlanner,
};
use dozer_types::types::{Record, Schema, SchemaIdentifier};

pub struct LmdbQueryHandler<'a> {
    db: &'a Database,
    indexer_db: &'a Database,
    txn: &'a RoTransaction<'a>,
}
impl<'a> LmdbQueryHandler<'a> {
    pub fn new(db: &'a Database, indexer_db: &'a Database, txn: &'a RoTransaction) -> Self {
        Self {
            db,
            indexer_db,
            txn,
        }
    }

    pub fn query(&self, schema: &Schema, query: &QueryExpression) -> anyhow::Result<Vec<Record>> {
        let planner = QueryPlanner {};
        let execution = planner.plan(schema, query)?;
        let records = match execution {
            ExecutionStep::IndexScan(index_scan) => self.query_with_secondary_index(
                &schema.identifier.clone().context("schema_id expected")?,
                index_scan,
                query.limit,
                query.skip,
            )?,
            ExecutionStep::SeqScan(seq_scan) => {
                self.iterate_and_deserialize(query.limit, query.skip)?
            }
        };

        Ok(records)
    }

    pub fn iterate_and_deserialize(
        &self,
        limit: usize,
        skip: usize,
    ) -> anyhow::Result<Vec<Record>> {
        let cursor = self.txn.open_ro_cursor(*self.db)?;
        let mut cache_iterator = CacheIterator::new(&cursor, None, true);
        // cache_iterator.skip(skip);

        let mut records = vec![];
        let mut idx = 0;
        loop {
            let rec = cache_iterator.next();
            if skip < idx {
                //
            } else if rec.is_some() && idx < limit {
                if let Some((_key, val)) = rec {
                    let rec = bincode::deserialize::<Record>(val)?;
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
        schema_identifier: &SchemaIdentifier,
        index_scan: IndexScan,
        limit: usize,
        skip: usize,
    ) -> anyhow::Result<Vec<Record>> {
        // TODO: Change logic based on typ

        let fields = index_scan
            .fields
            .iter()
            .map(|f| match f {
                Some(f) => Some(bincode::serialize(f).unwrap()),
                None => None,
            })
            .collect();

        let starting_key =
            index::get_secondary_index(schema_identifier.id, &index_scan.index_def.fields, &fields);
        let cursor = self.txn.open_ro_cursor(*self.indexer_db)?;

        let mut cache_iterator = CacheIterator::new(&cursor, Some(starting_key), true);
        let mut pkeys = vec![];
        let mut idx = 0;
        loop {
            if skip < idx && idx < limit {
                let tuple = cache_iterator.next();

                // Check if the tuple returns a value
                if let Some((key, val)) = tuple {
                    // Compare partial key
                    if self.compare_key(key, &index_scan) {
                        let rec = helper::get(self.txn, self.db, &val)?;
                        pkeys.push(rec);
                        idx += 1;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(pkeys)
    }

    fn compare_key(&self, _key: &[u8], _index_scan: &IndexScan) -> bool {
        // match self.value_to_compare {
        //             Some(ref value_to_compare) => {
        //                 // TODO: find a better implementation
        //                 // Find for partial matches if iterating on a query
        //                 if let Some(_idx) = gs_find(key, value_to_compare) {
        //                     Some(val.to_vec())
        //                 } else {
        //                     None
        //                 }
        //             }
        //             None => Some(val.to_vec()),
        //         }
        true
    }
}
