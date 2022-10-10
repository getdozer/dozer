use lmdb::{Database, RoTransaction, Transaction};

use super::{iterator::CacheIterator, lmdb_planner::LmdbQueryPlanner};
use crate::cache::{
    expression::{FilterExpression, QueryExpression},
    planner::QueryPlanner,
};
use dozer_types::types::{Record, Schema};

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
        let pkeys = match query.filter {
            FilterExpression::None => self.iterate_and_deserialize(query.limit, query.skip)?,

            _ => {
                let planner = LmdbQueryPlanner::new(&self.txn, &self.indexer_db);
                planner.execute(schema, query)?
            }
        };
        Ok(pkeys)
    }

    pub fn iterate_and_deserialize(
        &self,
        limit: usize,
        skip: usize,
    ) -> anyhow::Result<Vec<Record>> {
        let cursor = self.txn.open_ro_cursor(*self.db)?;
        let mut cache_iterator = CacheIterator::new(&cursor, None, None, true);
        // cache_iterator.skip(skip);

        let mut records = vec![];
        let mut idx = 0;
        loop {
            let rec = cache_iterator.next();
            if skip < idx {
                //
            } else if rec.is_some() && idx < limit {
                let rec = bincode::deserialize::<Record>(&rec.unwrap())?;
                records.push(rec);
            } else {
                break;
            }
            idx += 1;
        }
        Ok(records)
    }
    // fn query_with_secondary_index(
    //     &self,
    //     schema_identifier: &SchemaIdentifier,
    //     field_idx: usize,
    //     operator: &Operator,
    //     field: &Field,
    //     no_of_rows: Option<usize>,
    // ) -> anyhow::Result<Vec<Record>> {
    //     // TODO: Change logic based on typ
    //     let _typ = Self::get_index_type(operator);

    //     let field_to_compare = bincode::serialize(&field)?;

    //     let starting_key =
    //         helper::get_secondary_index(schema_identifier.id, &field_idx, &field_to_compare);

    //     let ascending = match operator {
    //         Operator::LT | Operator::LTE => false,
    //         // changes the order
    //         Operator::GT | Operator::GTE | Operator::EQ => true,
    //         // doesn't impact the order
    //         Operator::Contains | Operator::MatchesAny | Operator::MatchesAll => true,
    //     };
    //     let cursor = self.txn.open_ro_cursor(*self.indexer_db)?;

    //     let mut cache_iterator = CacheIterator::new(
    //         &cursor,
    //         Some(starting_key),
    //         Some(field_to_compare),
    //         ascending,
    //     );
    //     let mut pkeys = vec![];
    //     let mut idx = 0;
    //     loop {
    //         let key = cache_iterator.next();
    //         if key.is_some() {
    //             let rec = lmdb_helper::get(self.txn, self.db, &key.context("pley expected")?)?;
    //             pkeys.push(rec);
    //         } else {
    //             break;
    //         }
    //         idx += 1;
    //     }
    //     Ok(pkeys)
    // }
}
