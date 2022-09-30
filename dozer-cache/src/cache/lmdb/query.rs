use anyhow::{bail, Ok};
use lmdb::{Cursor, Database, Environment, Transaction};

use dozer_types::types::{Field, FieldDefinition, IndexType, Record, Schema, SchemaIdentifier};

use crate::cache::{
    expression::{Comparator, Expression},
    Cache,
};

use super::cache::LmdbCache;
use super::indexer::get_secondary_index;
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
                let field_defs: Vec<&FieldDefinition> = schema
                    .fields
                    .iter()
                    .filter(|fd| fd.name == column)
                    .collect();
                let field_def = field_defs.get(0).unwrap();
                let pkeys = self._query(schema.identifier.unwrap(), comparator, field)?;
                pkeys
            }
            Expression::Combination(operator, exp1, exp2) => todo!(),
        };
        Ok(pkeys)
    }

    fn _query(
        &self,
        schema_identifier: SchemaIdentifier,
        comparator: Comparator,
        field: Field,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let typ = Self::get_index_type(&comparator);
        let field_key = bincode::serialize(&field).unwrap();
        let indx = get_secondary_index(&schema_identifier, &typ, &field_key);

        let txn = self.env.begin_ro_txn()?;
        let cursor = txn.open_ro_cursor(*self.db)?;

        cursor.cursor();

        let op = match comparator {
            Comparator::LT | Comparator::LTE => MDB_PREV_DUP,
            Comparator::GT | Comparator::GTE | Comparator::EQ => MDB_NEXT_DUP,
        };

        let mut pkeys = vec![];
        while let (key, val) = cursor.get(None, None, op).unwrap() {
            match key {
                Some(key) => {
                    if let Some(_idx) = gs_find(key, &field_key) {
                        pkeys.push(val.to_vec())
                    }
                }
                None => bail!("key not found"),
            }
        }

        Ok(pkeys)
    }

    fn get_index_type(comparator: &Comparator) -> IndexType {
        IndexType::SortedInverted
    }
}
