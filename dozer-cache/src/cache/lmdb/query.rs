use anyhow::{bail, Ok};
use lmdb::{Cursor, Database, Environment, Transaction};

use dozer_types::types::{Field, FieldDefinition, IndexType, Schema, SchemaIdentifier};

use crate::cache::{
    expression::{Comparator, Expression},
    get_secondary_index,
};

use galil_seiferas::gs_find;
pub struct QueryHandler<'a> {
    env: &'a Environment,
    db: &'a Database,
}
pub const MDB_NEXT: u32 = 8;
pub const MDB_NEXT_DUP: u32 = 9;
pub const MDB_NEXT_MULTIPLE: u32 = 10;
pub const MDB_NEXT_NODUP: u32 = 11;
pub const MDB_PREV: u32 = 12;
pub const MDB_PREV_DUP: u32 = 13;
pub const MDB_PREV_NODUP: u32 = 14;

impl<'a> QueryHandler<'a> {
    pub fn new(env: &'a Environment, db: &'a Database) -> Self {
        Self { env, db }
    }

    pub fn query(&self, schema: Schema, exp: Expression) -> anyhow::Result<Vec<Vec<u8>>> {
        let pkeys = match exp {
            Expression::Simple(column, comparator, field) => {
                let field_defs: Vec<(usize, &FieldDefinition)> = schema
                    .fields
                    .iter()
                    .enumerate()
                    .filter(|(_field_idx, fd)| fd.name == column)
                    .collect();
                let field_def = field_defs.get(0).unwrap();
                let pkeys =
                    self._query(schema.identifier.unwrap(), field_def.0, comparator, field)?;
                pkeys
            }
            Expression::Combination(_operator, _exp1, _exp2) => todo!(),
        };
        Ok(pkeys)
    }

    fn _query(
        &self,
        schema_identifier: SchemaIdentifier,
        field_idx: usize,
        comparator: Comparator,
        field: Field,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        // TODO: Change logic based on typ
        let _typ = Self::get_index_type(&comparator);

        let field_val = bincode::serialize(&field).unwrap();

        let indx = get_secondary_index(schema_identifier.id, &field_idx, &field_val);

        let txn = self.env.begin_ro_txn()?;
        let cursor = txn.open_ro_cursor(*self.db)?;

        let op = match comparator {
            Comparator::LT | Comparator::LTE => MDB_PREV_DUP,
            Comparator::GT | Comparator::GTE | Comparator::EQ => MDB_NEXT_DUP,
        };

        let mut pkeys = vec![];
        loop {
            let (key, val) = cursor.get(Some(&indx), None, op)?;
            match key {
                Some(key) => {
                    if let Some(_idx) = gs_find(key, &field_val) {
                        pkeys.push(val.to_vec())
                    } else {
                        break;
                    }
                }
                None => {
                    bail!("key not found")
                }
            }
        }

        Ok(pkeys)
    }

    fn get_index_type(_comparator: &Comparator) -> IndexType {
        IndexType::SortedInverted
    }
}
