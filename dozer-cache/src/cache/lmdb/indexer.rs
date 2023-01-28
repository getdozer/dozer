use crate::errors::{CacheError, IndexError};
use dozer_types::{
    parking_lot::RwLock,
    types::{Field, IndexDefinition, Record, Schema},
};
use lmdb::{RwTransaction, Transaction};
use std::sync::Arc;
use unicode_segmentation::UnicodeSegmentation;

use crate::cache::index::{self, get_full_text_secondary_index};

use super::cache::SecondaryIndexDatabases;

pub struct Indexer {
    pub secondary_indexes: Arc<RwLock<SecondaryIndexDatabases>>,
}
impl Indexer {
    pub fn build_indexes(
        &self,
        parent_txn: &mut RwTransaction,
        record: &Record,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
        id: [u8; 8],
    ) -> Result<(), CacheError> {
        let schema_id = schema
            .identifier
            .ok_or(CacheError::SchemaIdentifierNotFound)?;

        let mut txn = parent_txn
            .begin_nested_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;

        if secondary_indexes.is_empty() {
            return Err(CacheError::IndexError(IndexError::MissingSecondaryIndexes));
        }
        for (idx, index) in secondary_indexes.iter().enumerate() {
            let db = *self
                .secondary_indexes
                .read()
                .get(&(schema_id, idx))
                .ok_or(CacheError::SecondaryIndexDatabaseNotFound)?;

            match index {
                IndexDefinition::SortedInverted(fields) => {
                    let secondary_key = Self::_build_index_sorted_inverted(fields, &record.values);
                    db.insert(&mut txn, &secondary_key, id)?;
                }
                IndexDefinition::FullText(field_index) => {
                    for secondary_key in
                        Self::_build_indices_full_text(*field_index, &record.values)?
                    {
                        db.insert(&mut txn, &secondary_key, id)?;
                    }
                }
            }
        }
        txn.commit()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;
        Ok(())
    }

    pub fn delete_indexes(
        &self,
        txn: &mut RwTransaction,
        record: &Record,
        schema: &Schema,
        secondary_indexes: &[IndexDefinition],
        id: [u8; 8],
    ) -> Result<(), CacheError> {
        let schema_id = schema
            .identifier
            .ok_or(CacheError::SchemaIdentifierNotFound)?;
        for (idx, index) in secondary_indexes.iter().enumerate() {
            let db = *self
                .secondary_indexes
                .read()
                .get(&(schema_id, idx))
                .ok_or(CacheError::SecondaryIndexDatabaseNotFound)?;

            match index {
                IndexDefinition::SortedInverted(fields) => {
                    let secondary_key = Self::_build_index_sorted_inverted(fields, &record.values);
                    db.delete(txn, &secondary_key, id)?;
                }
                IndexDefinition::FullText(field_index) => {
                    for secondary_key in
                        Self::_build_indices_full_text(*field_index, &record.values)?
                    {
                        db.delete(txn, &secondary_key, id)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn _build_index_sorted_inverted(fields: &[usize], values: &[Field]) -> Vec<u8> {
        let values = fields
            .iter()
            .copied()
            .filter_map(|index| (values.get(index)))
            .collect::<Vec<_>>();
        // `values.len() == 1` criteria must be kept the same with `comparator.rs`.
        index::get_secondary_index(&values, values.len() == 1)
    }

    fn _build_indices_full_text(
        field_index: usize,
        values: &[Field],
    ) -> Result<Vec<Vec<u8>>, CacheError> {
        let Some(field) = values.get(field_index) else {
            return Err(CacheError::IndexError(IndexError::FieldIndexOutOfRange));
        };

        let string = match field {
            Field::String(string) => string,
            Field::Text(string) => string,
            Field::Null => {
                let empty_string = "";
                empty_string
            }
            _ => {
                return Err(CacheError::IndexError(IndexError::FieldNotCompatibleIndex(
                    field_index,
                )))
            }
        };

        Ok(string
            .unicode_words()
            .map(get_full_text_secondary_index)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::{
        lmdb::tests::utils as lmdb_utils, lmdb::CacheOptions, test_utils, Cache, LmdbCache,
    };

    use super::*;

    #[test]
    fn test_secondary_indexes() {
        let cache = LmdbCache::new(CacheOptions::default()).unwrap();
        let (schema, secondary_indexes) = test_utils::schema_1();

        cache
            .insert_schema("sample", &schema, &secondary_indexes)
            .unwrap();

        let items = vec![
            (1, Some("a".to_string()), Some(521)),
            (2, Some("a".to_string()), None),
            (3, None, Some(521)),
            (4, None, None),
        ];

        for val in items.clone() {
            lmdb_utils::insert_rec_1(&cache, &schema, val);
        }
        // No of index dbs
        let indexes = lmdb_utils::get_indexes(&cache);

        let index_count = indexes.iter().flatten().count();
        let expected_count = secondary_indexes.len();
        // 3 columns, 1 compound, 1 descending
        assert_eq!(
            indexes.len(),
            expected_count,
            "Must create db for each index"
        );

        assert_eq!(
            index_count,
            items.len() * expected_count,
            "Must index each field"
        );

        for a in [1i64, 2, 3, 4] {
            cache.delete(&Field::Int(a).encode()).unwrap();
        }

        assert_eq!(
            lmdb_utils::get_indexes(&cache)
                .into_iter()
                .flatten()
                .count(),
            0,
            "Must delete every index"
        );
    }

    #[test]
    fn test_build_indices_full_text() {
        let field_index = 0;
        assert_eq!(
            Indexer::_build_indices_full_text(
                field_index,
                &[Field::String("today is a good day".into())]
            )
            .unwrap(),
            vec![
                get_full_text_secondary_index("today"),
                get_full_text_secondary_index("is"),
                get_full_text_secondary_index("a"),
                get_full_text_secondary_index("good"),
                get_full_text_secondary_index("day"),
            ]
        );
    }
}
