use crate::errors::{CacheError, IndexError, QueryError};
use dozer_types::types::{Field, IndexDefinition, Record, Schema, SortDirection};
use lmdb::{Database, RwTransaction, Transaction, WriteFlags};
use std::sync::Arc;
use unicode_segmentation::UnicodeSegmentation;

use crate::cache::index::{self, get_full_text_secondary_index};

use super::cache::IndexMetaData;

pub struct Indexer {
    pub primary_index: Database,
    pub index_metadata: Arc<IndexMetaData>,
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
        let mut txn = parent_txn
            .begin_nested_txn()
            .map_err(|e| CacheError::InternalError(Box::new(e)))?;

        if !schema.primary_index.is_empty() {
            let primary_key = index::get_primary_key(&schema.primary_index, &record.values);
            txn.put(self.primary_index, &primary_key, &id, WriteFlags::default())
                .map_err(|e| CacheError::QueryError(QueryError::InsertValue(e)))?;
        }

        if secondary_indexes.is_empty() {
            return Err(CacheError::IndexError(IndexError::MissingSecondaryIndexes));
        }
        for (idx, index) in secondary_indexes.iter().enumerate() {
            let db = self.index_metadata.get_db(schema, idx);

            match index {
                IndexDefinition::SortedInverted(fields) => {
                    let secondary_key = Self::_build_index_sorted_inverted(fields, &record.values);
                    txn.put(db, &secondary_key, &id, WriteFlags::default())
                        .map_err(QueryError::InsertValue)?;
                }
                IndexDefinition::FullText(field_index) => {
                    for secondary_key in
                        Self::_build_indices_full_text(*field_index, &record.values)?
                    {
                        txn.put(db, &secondary_key, &id, WriteFlags::default())
                            .map_err(QueryError::InsertValue)?;
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
        primary_key: &[u8],
        id: [u8; 8],
    ) -> Result<(), CacheError> {
        txn.del(self.primary_index, &primary_key, None)
            .map_err(QueryError::DeleteValue)?;

        for (idx, index) in secondary_indexes.iter().enumerate() {
            let db = self.index_metadata.get_db(schema, idx);

            match index {
                IndexDefinition::SortedInverted(fields) => {
                    let secondary_key = Self::_build_index_sorted_inverted(fields, &record.values);
                    txn.del(db, &secondary_key, Some(&id))
                        .map_err(QueryError::DeleteValue)?;
                }
                IndexDefinition::FullText(field_index) => {
                    for secondary_key in
                        Self::_build_indices_full_text(*field_index, &record.values)?
                    {
                        txn.del(db, &secondary_key, Some(&id))
                            .map_err(QueryError::DeleteValue)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn _build_index_sorted_inverted(
        fields: &[(usize, SortDirection)],
        values: &[Field],
    ) -> Vec<u8> {
        let values = fields
            .iter()
            .copied()
            .filter_map(|(index, direction)| (values.get(index).map(|value| (value, direction))))
            .collect::<Vec<_>>();
        // `values.len() == 1` criteria must be kept the same with `comparator.rs`.
        index::get_secondary_index(&values, values.len() == 1)
    }

    fn _build_indices_full_text(
        field_index: usize,
        values: &[Field],
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

        let items: Vec<(i64, String, i64)> = vec![
            (1, "a".to_string(), 521),
            (2, "a".to_string(), 521),
            (3, "a".to_string(), 521),
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

        for a in [1i64, 2, 3] {
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
