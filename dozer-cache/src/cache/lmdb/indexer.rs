use anyhow::{Context, Ok};
use dozer_types::types::{Field, IndexDefinition, Record, Schema, SchemaIdentifier};
use lmdb::{Database, RwTransaction, Transaction, WriteFlags};

use crate::cache::get_secondary_index;

pub struct Indexer<'a> {
    db: &'a Database,
}
impl<'a> Indexer<'a> {
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    pub fn build_indexes(
        &self,
        parent_txn: &'a mut RwTransaction,
        rec: &Record,
        schema: &Schema,
        pkey: Vec<u8>,
    ) -> anyhow::Result<()> {
        let mut txn = parent_txn.begin_nested_txn()?;

        let identifier = &schema
            .identifier
            .to_owned()
            .context("schema_id is expected")?;
        for index in schema.secondary_indexes.iter() {
            let keys = self._build_index(index, rec, identifier)?;

            for secondary_key in keys.iter() {
                let _typ = &index.typ;

                txn.put::<Vec<u8>, Vec<u8>>(*self.db, secondary_key, &pkey, WriteFlags::default())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    fn _build_index(
        &self,
        index: &IndexDefinition,
        rec: &Record,
        identifier: &SchemaIdentifier,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let keys = match index.typ {
            dozer_types::types::IndexType::SortedInverted => {
                self._build_index_sorted_inverted(identifier, &index.fields, &rec.values)
            }
            dozer_types::types::IndexType::HashInverted => todo!(),
        };

        Ok(keys)
    }

    fn _build_index_sorted_inverted(
        &self,
        identifier: &SchemaIdentifier,
        index_fields: &[usize],
        values: &[Field],
    ) -> Vec<Vec<u8>> {
        let keys: Vec<Vec<u8>> = index_fields
            .iter()
            .map(|idx| {
                let field = values[*idx].clone();
                let field_val: Vec<u8> = bincode::serialize(&field).unwrap();

                get_secondary_index(identifier.id, idx, &field_val)
            })
            .collect();
        keys
    }
}
