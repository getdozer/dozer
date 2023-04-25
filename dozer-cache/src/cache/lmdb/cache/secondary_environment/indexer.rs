use crate::errors::{CacheError, IndexError};

use dozer_storage::lmdb::RwTransaction;
use dozer_types::types::{Field, IndexDefinition, Record};

use dozer_storage::LmdbMultimap;

use itertools::Itertools;
use unicode_segmentation::UnicodeSegmentation;

use crate::cache::index::{self, get_full_text_secondary_index};

pub fn build_index(
    txn: &mut RwTransaction,
    database: LmdbMultimap<Vec<u8>, u64>,
    record: &Record,
    index_definition: &IndexDefinition,
    operation_id: u64,
) -> Result<(), CacheError> {
    match index_definition {
        IndexDefinition::SortedInverted(fields) => {
            let secondary_key = build_index_sorted_inverted(fields, &record.values);
            // Ignore existing pair.
            database.insert(txn, &secondary_key, &operation_id)?;
        }
        IndexDefinition::FullText(field_index) => {
            for secondary_key in build_indices_full_text(*field_index, &record.values)? {
                // Ignore existing pair.
                database.insert(txn, &secondary_key, &operation_id)?;
            }
        }
    }
    Ok(())
}

pub fn delete_index(
    txn: &mut RwTransaction,
    database: LmdbMultimap<Vec<u8>, u64>,
    record: &Record,
    index_definition: &IndexDefinition,
    operation_id: u64,
) -> Result<(), CacheError> {
    match index_definition {
        IndexDefinition::SortedInverted(fields) => {
            let secondary_key = build_index_sorted_inverted(fields, &record.values);
            // Ignore if not found.
            database.remove(txn, &secondary_key, &operation_id)?;
        }
        IndexDefinition::FullText(field_index) => {
            for secondary_key in build_indices_full_text(*field_index, &record.values)? {
                // Ignore if not found.
                database.remove(txn, &secondary_key, &operation_id)?;
            }
        }
    }
    Ok(())
}

fn build_index_sorted_inverted(fields: &[usize], values: &[Field]) -> Vec<u8> {
    let values = fields
        .iter()
        .copied()
        .filter_map(|index| (values.get(index)))
        .collect::<Vec<_>>();
    // `values.len() == 1` criteria must be kept the same with `comparator.rs`.
    index::get_secondary_index(&values, values.len() == 1)
}

fn build_indices_full_text(
    field_index: usize,
    values: &[Field],
) -> Result<Vec<Vec<u8>>, CacheError> {
    let Some(field) = values.get(field_index) else {
            return Err(CacheError::Index(IndexError::FieldIndexOutOfRange));
        };

    let string = match field {
        Field::String(string) => string,
        Field::Text(string) => string,
        Field::Null => "",
        _ => {
            return Err(CacheError::Index(IndexError::FieldNotCompatibleIndex(
                field_index,
            )))
        }
    };

    Ok(string
        .unicode_words()
        .map(get_full_text_secondary_index)
        .unique()
        .collect())
}

#[cfg(test)]
mod tests {
    use crate::cache::{
        lmdb::tests::utils::{self as lmdb_utils, create_cache},
        test_utils, RwCache,
    };

    use super::*;

    #[test]
    fn test_secondary_indexes() {
        let (mut cache, indexing_thread_pool, schema, secondary_indexes) =
            create_cache(test_utils::schema_1);

        let items = vec![
            (1, Some("a".to_string()), Some(521)),
            (2, Some("a".to_string()), None),
            (3, None, Some(521)),
            (4, None, None),
        ];

        for val in items.clone() {
            lmdb_utils::insert_rec_1(&mut cache, &schema, val);
        }
        cache.commit().unwrap();
        indexing_thread_pool.lock().wait_until_catchup();

        // No of index dbs
        let index_counts = lmdb_utils::get_index_counts(&cache);
        let expected_count = secondary_indexes.len();
        assert_eq!(index_counts.len(), expected_count);

        // 3 columns, 1 compound, 1 descending
        assert_eq!(
            index_counts.iter().sum::<usize>(),
            items.len() * expected_count,
        );

        for a in [1i64, 2, 3, 4] {
            let record = Record {
                schema_id: schema.identifier,
                values: vec![Field::Int(a), Field::Null, Field::Null],
            };
            cache.delete(&record).unwrap();
        }
        cache.commit().unwrap();
        indexing_thread_pool.lock().wait_until_catchup();

        assert_eq!(
            lmdb_utils::get_index_counts(&cache)
                .into_iter()
                .sum::<usize>(),
            0,
            "Must delete every index"
        );
    }

    #[test]
    fn test_build_indices_full_text() {
        let field_index = 0;
        assert_eq!(
            build_indices_full_text(field_index, &[Field::String("today is a good day".into())])
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

    #[test]
    fn test_full_text_secondary_index_with_duplicated_words() {
        let (mut cache, indexing_thread_pool, schema, _) =
            create_cache(test_utils::schema_full_text);

        let items = vec![(
            Some("another test".to_string()),
            Some("regular test regular".to_string()),
        )];

        for val in items {
            lmdb_utils::insert_full_text(&mut cache, &schema, val);
        }

        {
            let a = "another test".to_string();
            let record = Record {
                schema_id: schema.identifier,
                values: vec![Field::String(a), Field::Null],
            };
            cache.delete(&record).unwrap();
        }

        cache.commit().unwrap();
        indexing_thread_pool.lock().wait_until_catchup();

        assert_eq!(
            lmdb_utils::get_index_counts(&cache)
                .into_iter()
                .sum::<usize>(),
            0,
            "Must delete every index"
        );
    }
}
