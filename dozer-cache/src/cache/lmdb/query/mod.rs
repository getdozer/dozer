use anyhow::Context;
use lmdb::{Database, RoTransaction, Transaction};
pub mod iterator;

use dozer_types::types::{Field, FieldDefinition, IndexType, Record, Schema, SchemaIdentifier};

use crate::cache::{
    expression::{FilterExpression, Operator},
    CacheHelper,
};

use iterator::CacheIterator;
pub struct QueryHandler<'a> {
    db: &'a Database,
    indexer_db: &'a Database,
    txn: &'a RoTransaction<'a>,
}

impl<'a> QueryHandler<'a> {
    pub fn new(db: &'a Database, indexer_db: &'a Database, txn: &'a RoTransaction) -> Self {
        Self {
            db,
            indexer_db,
            txn,
        }
    }

    pub fn get(&self, key: &[u8], txn: &RoTransaction) -> anyhow::Result<Record> {
        let rec = txn.get(*self.db, &key)?;
        let rec: Record = bincode::deserialize(rec)?;
        Ok(rec)
    }

    pub fn query(
        &self,
        schema: &Schema,
        exp: &FilterExpression,
        no_of_rows: Option<usize>,
    ) -> anyhow::Result<Vec<Record>> {
        let pkeys = match exp {
            FilterExpression::None => self.list(true, no_of_rows)?,
            FilterExpression::Simple(column, operator, field) => {
                let field_defs: Vec<(usize, &FieldDefinition)> = schema
                    .fields
                    .iter()
                    .enumerate()
                    .filter(|(_field_idx, fd)| fd.name == *column)
                    .collect();
                let field_def = field_defs.get(0).unwrap();

                self.query_with_secondary_index(
                    &schema.identifier.to_owned().context("schema_id expected")?,
                    field_def.0,
                    operator,
                    field,
                    no_of_rows,
                )?
            }
            // Expression::And(_exp1, _exp2) => todo!(),
            // Expression::Or(_exp1, _exp2) => todo!(),
        };
        Ok(pkeys)
    }

    fn list(&self, ascending: bool, no_of_rows: Option<usize>) -> anyhow::Result<Vec<Record>> {
        let cursor = self.txn.open_ro_cursor(*self.db)?;
        let mut cache_iterator = CacheIterator::new(&cursor, None, None, ascending, no_of_rows);

        let mut records = vec![];
        loop {
            let rec = cache_iterator.next();
            if rec.is_some() {
                let rec: Record = bincode::deserialize(&rec.unwrap())?;
                records.push(rec);
            } else {
                break;
            }
        }
        Ok(records)
    }

    fn query_with_secondary_index(
        &self,
        schema_identifier: &SchemaIdentifier,
        field_idx: usize,
        operator: &Operator,
        field: &Field,
        no_of_rows: Option<usize>,
    ) -> anyhow::Result<Vec<Record>> {
        // TODO: Change logic based on typ
        let _typ = Self::get_index_type(operator);

        let field_to_compare = bincode::serialize(&field)?;

        let starting_key =
            CacheHelper::get_secondary_index(schema_identifier.id, &field_idx, &field_to_compare);

        let ascending = match operator {
            Operator::LT | Operator::LTE => false,
            // changes the order
            Operator::GT | Operator::GTE | Operator::EQ => true,
            // doesn't impact the order
            Operator::Contains | Operator::MatchesAny | Operator::MatchesAll => true,
        };
        let cursor = self.txn.open_ro_cursor(*self.indexer_db)?;

        let mut cache_iterator = CacheIterator::new(
            &cursor,
            Some(starting_key),
            Some(field_to_compare),
            ascending,
            no_of_rows,
        );
        let mut pkeys = vec![];
        loop {
            let key = cache_iterator.next();
            if key.is_some() {
                let rec = self.get(&key.context("pley expected")?, self.txn)?;
                pkeys.push(rec);
            } else {
                break;
            }
        }
        Ok(pkeys)
    }

    fn get_index_type(_comparator: &Operator) -> IndexType {
        IndexType::SortedInverted
    }
}
