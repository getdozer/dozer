use dozer_types::bincode;
use dozer_types::types::{IndexDefinition, Record};

pub trait CacheIndex {
    // Builds one index based on index definition and record
    fn build(index: &IndexDefinition, rec: &Record) -> Vec<Vec<u8>>;

    fn get_key(schema_id: u32, field_idx: &usize, field_val: &[u8]) -> Vec<u8>;
}

use dozer_types::types::Field;

pub fn get_primary_key(primary_index: &[usize], values: &[Field]) -> Vec<u8> {
    let key: Vec<Vec<u8>> = primary_index
        .iter()
        .map(|idx| {
            let field = &values[*idx];
            let encoded: Vec<u8> = bincode::serialize(field).unwrap();
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

pub fn get_secondary_index(field_val: &[Vec<u8>]) -> Vec<u8> {
    // Put a '#' at first so the result is never empty.
    let field_val: Vec<Vec<u8>> = std::iter::once(vec![b'#'])
        .chain(field_val.iter().cloned())
        .collect();
    field_val.join("#".as_bytes())
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
        assert!(!super::get_secondary_index(&[]).is_empty());
        assert!(!super::get_secondary_index(&[vec![]]).is_empty());
    }

    #[test]
    fn test_get_full_text_secondary_index() {
        assert_eq!(get_full_text_secondary_index("foo"), b"foo",);
    }
}
