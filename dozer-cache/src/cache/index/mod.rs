use std::cmp::Ordering;

use dozer_types::bincode;
use dozer_types::types::{IndexDefinition, Record, SortDirection};

pub trait CacheIndex {
    // Builds one index based on index definition and record
    fn build(index: &IndexDefinition, rec: &Record) -> Vec<Vec<u8>>;

    fn get_key(schema_id: u32, field_idx: &usize, field_val: &[u8]) -> Vec<u8>;
}

use dozer_types::types::Field;

use crate::errors::CacheError;

use super::lmdb::comparator::compared_without_composite_key;

pub fn get_primary_key(primary_index: &[usize], values: &[Field]) -> Vec<u8> {
    let key: Vec<Vec<u8>> = primary_index
        .iter()
        .map(|idx| {
            let field = &values[*idx];
            let encoded: Vec<u8> = field.to_bytes().unwrap();
            encoded
        })
        .collect();

    key.join("#".as_bytes())
}

pub fn has_primary_key_changed(
    primary_index: &[usize],
    old_values: &[Field],
    new_values: &[Field],
) -> bool {
    primary_index
        .iter()
        .any(|idx| old_values[*idx] != new_values[*idx])
}

pub fn get_secondary_index(fields: &[(&Field, SortDirection)]) -> Result<Vec<u8>, CacheError> {
    // This criteria must be kept consistent with `comparator.rs`.
    if fields.len() == 1
        && (fields[0].1 == SortDirection::Ascending || compared_without_composite_key(fields[0].0))
    {
        Ok(fields[0].0.to_bytes()?.to_vec())
    } else {
        bincode::serialize(fields).map_err(CacheError::map_serialization_error)
    }
}

pub fn compare_composite_secondary_index(a: &[u8], b: &[u8]) -> bincode::Result<Ordering> {
    let mut a_fields = bincode::deserialize::<Vec<(Field, SortDirection)>>(a)?.into_iter();
    let mut b_fields = bincode::deserialize::<Vec<(Field, SortDirection)>>(b)?.into_iter();
    Ok(loop {
        match (a_fields.next(), b_fields.next()) {
            (Some((a, a_direction)), Some((b, b_direction))) => {
                debug_assert!(a_direction == b_direction);
                match a.cmp(&b) {
                    Ordering::Equal => continue,
                    ordering => match a_direction {
                        SortDirection::Ascending => break ordering,
                        SortDirection::Descending => break ordering.reverse(),
                    },
                }
            }
            (Some(_), None) => break Ordering::Greater,
            (None, Some(_)) => break Ordering::Less,
            (None, None) => break Ordering::Equal,
        }
    })
}

pub fn get_full_text_secondary_index(token: &str) -> Vec<u8> {
    token.as_bytes().to_vec()
}

pub fn get_schema_reverse_key(name: &str) -> Vec<u8> {
    ["schema_name_".as_bytes(), name.as_bytes()].join("#".as_bytes())
}

#[cfg(test)]
mod tests {
    use super::get_full_text_secondary_index;

    #[test]
    fn secondary_index_is_never_empty() {
        assert!(!super::get_secondary_index(&[]).unwrap().is_empty());
    }

    #[test]
    fn test_get_full_text_secondary_index() {
        assert_eq!(get_full_text_secondary_index("foo"), b"foo",);
    }
}
