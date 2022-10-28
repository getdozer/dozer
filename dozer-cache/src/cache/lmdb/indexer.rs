use dozer_types::{
    bincode,
    errors::cache::{CacheError, IndexError, QueryError},
    types::{Field, Record, Schema, SchemaIdentifier},
};
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
    ) -> Result<(), CacheError> {
        let mut txn = parent_txn
            .begin_nested_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;

        let identifier = &schema
            .identifier
            .to_owned()
            .map_or(Err(CacheError::SchemaIdentifierNotFound), Ok)?;

        if schema.secondary_indexes.is_empty() {
            return Err(CacheError::IndexError(IndexError::MissingSecondaryIndexes));
        }
        for index in schema.secondary_indexes.iter() {
            match index {
                dozer_types::types::IndexDefinition::SortedInverted { fields } => {
                    let index_fields = fields.iter().map(|field| field.index).collect::<Vec<_>>();
                    let secondary_key =
                        self._build_index_sorted_inverted(identifier, &index_fields, &rec.values);
                    txn.put::<Vec<u8>, Vec<u8>>(
                        self.db,
                        &secondary_key,
                        &pkey,
                        WriteFlags::default(),
                    )
                    .map_err(|_e| CacheError::QueryError(QueryError::InsertValue))?;
                }
                dozer_types::types::IndexDefinition::HashInverted => todo!(),
                dozer_types::types::IndexDefinition::FullText { field_index } => {
                    for secondary_key in
                        self._build_indices_full_text(identifier, *field_index, &rec.values)?
                    {
                        txn.put(self.db, &secondary_key, &pkey, WriteFlags::default())
                            .map_err(|_e| CacheError::QueryError(QueryError::InsertValue))?;
                    }
                }
            }
        }
        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
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

    fn _build_indices_full_text(
        &self,
        identifier: &SchemaIdentifier,
        field_index: usize,
        fields: &[Field],
    ) -> Result<Vec<Vec<u8>>, CacheError> {
        let string = match &fields[field_index] {
            Field::String(string) => string,
            _ => return Err(CacheError::IndexError(IndexError::ExpectedStringFullText)),
        };

        Ok(string
            .unicode_words()
            .map(|token| get_full_text_secondary_index(identifier.id, field_index as _, token))
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
                    field_index,
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
