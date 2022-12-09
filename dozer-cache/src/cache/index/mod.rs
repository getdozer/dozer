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

pub fn get_primary_key(primary_index: &[usize], values: &[Field]) -> Vec<u8> {
    let key: Vec<Vec<u8>> = primary_index
        .iter()
        .map(|idx| values[*idx].encode())
        .collect();

    key.join("#".as_bytes())
}

/// Returns the secondary index key for a given set of fields.
///
/// We allow computing the secondary index key of "prefix" fields, so the user can filter the "prefix" fields using `Eq` filters,
/// and sort the filtering result using other fields.
///
/// In the meantime, we compute the key differently for single field indexes and compound indexes.
/// We'are not able to tell if certain `fields` belong to a single field index or compound index if its length is 1, hence the second parameter.
///
/// # Parameters
/// - `fields`: The fields to index.
/// - `is_single_field_index`: Whether the `fields` belong to a single field index. If `true`, `fields` must have length 1.
pub fn get_secondary_index(
    fields: &[(&Field, SortDirection)],
    is_single_field_index: bool,
) -> Result<Vec<u8>, CacheError> {
    debug_assert!(!is_single_field_index || fields.len() == 1);
    if is_single_field_index {
        Ok(fields[0].0.encode())
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
mod tests;
