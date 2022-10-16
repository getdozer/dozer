use anyhow::Context;
use galil_seiferas::gs_find;
use lmdb::{Database, RoTransaction, Transaction};

use super::{helper, iterator::CacheIterator};
use crate::cache::{
    expression::{ExecutionStep, IndexScan, QueryExpression},
    index,
    planner::QueryPlanner,
};
use dozer_types::types::{Record, Schema, SchemaIdentifier};

pub struct LmdbQueryHandler<'a> {
    db: Database,
    indexer_db: Database,
    txn: &'a RoTransaction<'a>,
}
impl<'a> LmdbQueryHandler<'a> {
    pub fn new(db: Database, indexer_db: Database, txn: &'a RoTransaction) -> Self {
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
            ExecutionStep::SeqScan(_seq_scan) => {
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
        let cursor = self.txn.open_ro_cursor(self.db)?;
        let mut cache_iterator = CacheIterator::new(&cursor, None, true);
        // cache_iterator.skip(skip);

        let mut records = vec![];
        let mut idx = 0;
        loop {
            let rec = cache_iterator.next();
            if skip > idx {
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

        let fields: Vec<Option<Vec<u8>>> = index_scan
            .fields
            .iter()
            .map(|f| f.as_ref().map(|f| bincode::serialize(f).unwrap()))
            .collect();

        let starting_key =
            index::get_secondary_index(schema_identifier.id, &index_scan.index_def.fields, &fields);
        let cursor = self.txn.open_ro_cursor(self.indexer_db)?;

        let mut cache_iterator = CacheIterator::new(&cursor, Some(&starting_key), true);
        let mut pkeys = vec![];
        let mut idx = 0;
        loop {
            if skip > idx {
            } else if idx < limit {
                let tuple = cache_iterator.next();

                // Check if the tuple returns a value
                if let Some((key, val)) = tuple {
                    // Compare partial key
                    if self.compare_key(key, &starting_key) {
                        let rec = helper::get(self.txn, self.db, val)?;
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

    fn compare_key(&self, key: &[u8], starting_key: &[u8]) -> bool {
        // TODO: find a better implementation
        // Find for partial matches if iterating on a query
        matches!(gs_find(key, starting_key), Some(_idx))
    }
}
