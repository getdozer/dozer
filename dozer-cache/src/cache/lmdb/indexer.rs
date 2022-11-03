use std::{collections::HashMap, sync::Arc};

use dozer_types::{
    errors::cache::{CacheError, IndexError, QueryError},
    types::{Field, IndexDefinition, Record, Schema, SchemaIdentifier},
};
use lmdb::{RwTransaction, Transaction, WriteFlags};
use unicode_segmentation::UnicodeSegmentation;

use crate::cache::index::{self, get_full_text_secondary_index};

use super::cache::IndexMetaData;

pub struct Indexer {
    pub index_metadata: Arc<IndexMetaData>,
}
impl Indexer {
    pub fn get_key(schema: &Schema, idx: usize) -> usize {
        schema.identifier.as_ref().unwrap().id as usize * 100000 + idx
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
        for (idx, index) in schema.secondary_indexes.iter().enumerate() {
            let db = self.index_metadata.get_db(schema, idx);

            match index {
                IndexDefinition::SortedInverted(fields) => {
                    // TODO: use `SortDirection`.
                    let fields: Vec<_> = fields.iter().map(|(index, _)| *index).collect();
                    let secondary_key =
                        self._build_index_sorted_inverted(identifier, &fields, &rec.values);
                    txn.put::<Vec<u8>, Vec<u8>>(db, &secondary_key, &pkey, WriteFlags::default())
                        .map_err(|_e| CacheError::QueryError(QueryError::InsertValue))?;
                }
                IndexDefinition::FullText(field_index) => {
                    for secondary_key in
                        self._build_indices_full_text(identifier, *field_index, &rec.values)?
                    {
                        txn.put(db, &secondary_key, &pkey, WriteFlags::default())
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
            .map(|(_, field)| Some(field.to_bytes().unwrap()))
            .collect();

        index::get_secondary_index(identifier.id, index_fields, &values)
    }

    fn _build_indices_full_text<'a>(
        &self,
        identifier: &SchemaIdentifier,
        field_index: usize,
        values: &'a [Field],
    ) -> Result<Vec<Vec<u8>>, CacheError> {
        let string = if let Some(field) = values.get(field_index) {
            if let Field::String(string) = field {
                string
            } else {
                return Err(CacheError::IndexError(IndexError::FieldNotCompatibleIndex(
                    field_index,
                )));
            }
        } else {
            return Err(CacheError::IndexError(IndexError::FieldIndexOutOfRange));
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
        lmdb::test_utils as lmdb_utils,
        lmdb::utils::{init_db, init_env},
        test_utils::{self, schema_0},
        Cache, LmdbCache,
    };

    use super::*;

    #[test]
    fn test_secondary_indexes() {
        let mut cache = LmdbCache::new(true);
        let schema = test_utils::schema_1();

        cache.insert_schema("sample", &schema).unwrap();

        let items: Vec<(i64, String, i64)> = vec![
            (1, "a".to_string(), 521),
            (2, "a".to_string(), 521),
            (3, "a".to_string(), 521),
        ];

        for val in items.clone() {
            lmdb_utils::insert_rec_1(&cache, &schema, val);
        }
        // No of indexes
        let indexes = lmdb_utils::get_indexes(&cache);
        // 3 columns, 1 compound
        assert_eq!(
            indexes.len(),
            items.len() * 4,
            "Must create index for each indexable field"
        );
    }

    #[test]
    fn test_build_indices_full_text() {
        let indexer = Indexer {
            index_metadata: Arc::new(IndexMetaData::new()),
        };
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
