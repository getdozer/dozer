use anyhow::{bail, Context, Ok};
use dozer_types::types::{Field, Record, Schema, SchemaIdentifier};
use lmdb::{Database, RwTransaction, Transaction, WriteFlags};
use unicode_segmentation::UnicodeSegmentation;

use crate::cache::index::{self, get_full_text_secondary_index};

pub struct Indexer {
    db: Database,
}
impl Indexer {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    pub fn build_indexes(
        &self,
        parent_txn: &mut RwTransaction,
        rec: &Record,
        schema: &Schema,
        pkey: Vec<u8>,
    ) -> anyhow::Result<()> {
        let mut txn = parent_txn.begin_nested_txn()?;

        let identifier = &schema
            .identifier
            .to_owned()
            .context("schema_id is expected")?;

        if schema.secondary_indexes.is_empty() {
            bail!("No secondary indexes defined.")
        }
        for index in schema.secondary_indexes.iter() {
            match index.typ {
                dozer_types::types::IndexType::SortedInverted => {
                    let secondary_key =
                        self._build_index_sorted_inverted(identifier, &index.fields, &rec.values);
                    txn.put::<Vec<u8>, Vec<u8>>(
                        self.db,
                        &secondary_key,
                        &pkey,
                        WriteFlags::default(),
                    )?;
                }
                dozer_types::types::IndexType::HashInverted => todo!(),
                dozer_types::types::IndexType::FullText => {
                    for secondary_key in
                        self._build_indices_full_text(identifier, &index.fields, &rec.values)?
                    {
                        txn.put(self.db, &secondary_key, &pkey, WriteFlags::default())?;
                    }
                }
            }
        }
        txn.commit()?;
        Ok(())
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

    fn _build_indices_full_text<'a>(
        &self,
        identifier: &SchemaIdentifier,
        index_fields: &[usize],
        values: &'a [Field],
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let mut fields = vec![];
        for field_index in index_fields {
            if let Some(field) = values.get(*field_index) {
                if let Field::String(string) = field {
                    fields.push((*field_index, string));
                } else {
                    bail!("Field {:?} cannot be indexed using full text", field);
                }
            }
        }

        let tokens = fields.iter().flat_map(|(field_index, string)| {
            string.unicode_words().map(|token| (*field_index, token))
        });

        Ok(tokens
            .map(|(field_index, token)| {
                get_full_text_secondary_index(identifier.id, field_index as _, token)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::{
        lmdb::utils::{init_db, init_env},
        test_utils::schema_0,
    };

    use super::*;

    #[test]
    fn test_build_indices_full_text() {
        let env = init_env(true).unwrap();
        let db = init_db(&env, None).unwrap();
        let indexer = Indexer { db };
        let schema = schema_0();

        let identifier = schema.identifier.as_ref().unwrap();
        let field_index = 0;
        assert_eq!(
            indexer
                ._build_indices_full_text(
                    identifier,
                    &[field_index],
                    &[Field::String("today is a good day".into())]
                )
                .unwrap(),
            vec![
                get_full_text_secondary_index(identifier.id, field_index as _, "today"),
                get_full_text_secondary_index(identifier.id, field_index as _, "is"),
                get_full_text_secondary_index(identifier.id, field_index as _, "a"),
                get_full_text_secondary_index(identifier.id, field_index as _, "good"),
                get_full_text_secondary_index(identifier.id, field_index as _, "day"),
            ]
        );
    }
}
