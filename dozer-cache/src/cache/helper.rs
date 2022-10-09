use dozer_types::types::Field;

pub struct CacheHelper {}
impl CacheHelper {
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

    pub fn get_secondary_index(schema_id: u32, field_idx: &usize, field_val: &[u8]) -> Vec<u8> {
        [
            "index_".as_bytes().to_vec(),
            schema_id.to_be_bytes().to_vec(),
            field_idx.to_be_bytes().to_vec(),
            field_val.to_vec(),
        ]
        .join("#".as_bytes())
    }
}
