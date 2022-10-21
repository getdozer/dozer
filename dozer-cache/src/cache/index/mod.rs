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

pub fn get_secondary_index(
    schema_id: u32,
    field_idx: &[usize],
    field_val: &[Option<Vec<u8>>],
) -> Vec<u8> {
    let field_val: Vec<Vec<u8>> = field_val
        .iter()
        .map(|f| match f {
            Some(f) => f.clone(),
            None => vec![],
        })
        .collect();
    let field_val = field_val.join("#".as_bytes());

    let field_idx: Vec<Vec<u8>> = field_idx
        .iter()
        .map(|idx| idx.to_be_bytes().to_vec())
        .collect();
    let field_idx = field_idx.join("#".as_bytes());

    [
        "index_".as_bytes().to_vec(),
        schema_id.to_be_bytes().to_vec(),
        field_idx,
        field_val,
    ]
    .join("#".as_bytes())
}

pub fn get_schema_reverse_key(name: &str) -> Vec<u8> {
    ["schema_name_".as_bytes(), name.as_bytes()].join("#".as_bytes())
}
