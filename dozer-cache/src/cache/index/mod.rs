use dozer_types::types::{IndexDefinition, Record};

pub trait CacheIndex {
    // Builds one index based on index definition and record
    fn build(index: &IndexDefinition, rec: &Record) -> Vec<Vec<u8>>;

    fn get_key(schema_id: u32, field_idx: &usize, field_val: &[u8]) -> Vec<u8>;
}
