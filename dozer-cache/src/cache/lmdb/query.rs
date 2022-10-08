use anyhow::Context;
use lmdb::{Database, RoTransaction, Transaction};

use dozer_types::types::{Field, FieldDefinition, IndexType, Record, Schema, SchemaIdentifier};

use crate::cache::{
    expression::{Comparator, Expression},
    get_secondary_index,
};

use super::cursor::CacheCursor;
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

    pub fn get(&self, key: &Vec<u8>, txn: &RoTransaction) -> anyhow::Result<Record> {
        let rec = txn.get(*self.db, &key)?;
        let rec: Record = bincode::deserialize(rec)?;
        Ok(rec)
    }

    pub fn query(
        &self,
        schema: &Schema,
        exp: &Expression,
        no_of_rows: usize,
    ) -> anyhow::Result<Vec<Record>> {
        let pkeys = match exp {
            Expression::None => self.list(true, no_of_rows)?,
            Expression::Simple(column, comparator, field) => {
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
                    comparator,
                    field,
                    no_of_rows,
                )?
            }
            Expression::Composite(_operator, _exp1, _exp2) => todo!(),
        };
        Ok(pkeys)
    }

    fn list(&self, ascending: bool, no_of_rows: usize) -> anyhow::Result<Vec<Record>> {
        let cursor = self.txn.open_ro_cursor(*self.db)?;
        let cache_cursor = CacheCursor::new(&cursor);
        let record_bufs = cache_cursor.get_records(None, None, ascending, no_of_rows)?;

        let mut records = vec![];
        for rec in record_bufs.iter() {
            let rec: Record = bincode::deserialize(rec)?;
            records.push(rec);
        }
        Ok(records)
    }

    fn query_with_secondary_index(
        &self,
        schema_identifier: &SchemaIdentifier,
        field_idx: usize,
        comparator: &Comparator,
        field: &Field,
        no_of_rows: usize,
    ) -> anyhow::Result<Vec<Record>> {
        // TODO: Change logic based on typ
        let _typ = Self::get_index_type(comparator);

        let field_to_compare = bincode::serialize(&field)?;

        let starting_key = get_secondary_index(schema_identifier.id, &field_idx, &field_to_compare);

        let ascending = match comparator {
            Comparator::LT | Comparator::LTE => false,
            Comparator::GT | Comparator::GTE | Comparator::EQ => true,
        };
        let cursor = self.txn.open_ro_cursor(*self.indexer_db)?;

        let cache_cursor = CacheCursor::new(&cursor);
        let pkeys = cache_cursor.get_records(
            Some(starting_key),
            Some(field_to_compare),
            ascending,
            no_of_rows,
        )?;
        let mut records = vec![];
        for key in pkeys.iter() {
            let rec = self.get(key, self.txn)?;
            records.push(rec);
        }
        Ok(records)
    }

    fn get_index_type(_comparator: &Comparator) -> IndexType {
        IndexType::SortedInverted
    }
}
