use anyhow::{Context, Ok};
use dozer_types::types::{Field, IndexDefinition, Record, Schema, SchemaIdentifier};
use lmdb::{Database, RwTransaction, Transaction, WriteFlags};

use crate::cache::index;

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
            let secondary_key = self._build_index(index, rec, identifier)?;

            txn.put::<Vec<u8>, Vec<u8>>(*self.db, &secondary_key, &pkey, WriteFlags::default())?;
        }
        txn.commit()?;
        Ok(())
    }

    fn _build_index(
        &self,
        index: &IndexDefinition,
        rec: &Record,
        identifier: &SchemaIdentifier,
    ) -> anyhow::Result<Vec<u8>> {
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
    ) -> Vec<u8> {
        let values: Vec<Option<Vec<u8>>> = values
            .iter()
            .enumerate()
            .filter(|(idx, _)| index_fields.contains(idx))
            .map(|(_, field)| Some(bincode::serialize(&field).unwrap()))
            .collect();

        index::get_secondary_index(identifier.id, index_fields, &values)
    }
}
