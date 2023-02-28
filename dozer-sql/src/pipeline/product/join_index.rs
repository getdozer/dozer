use dozer_types::{
    errors::types::DeserializationError,
    types::{Field, Record, Schema},
};

use crate::pipeline::errors::JoinError;

pub fn decode_lookup_key(
    lookup_key: &[u8],
) -> Result<(Option<u32>, Vec<Field>), DeserializationError> {
    let mut offset = 0;

    let record_version = u32::from_be_bytes([
        lookup_key[offset],
        lookup_key[offset + 1],
        lookup_key[offset + 2],
        lookup_key[offset + 3],
    ]);
    offset += 4;

    let version = if record_version != 0 {
        Some(record_version)
    } else {
        None
    };

    let mut values = vec![];
    while offset < lookup_key.len() {
        let field_length = u32::from_be_bytes([
            lookup_key[offset],
            lookup_key[offset + 1],
            lookup_key[offset + 2],
            lookup_key[offset + 3],
        ]);
        offset += 4;

        let field_bytes = &lookup_key[offset..offset + field_length as usize];
        offset += field_length as usize;

        let field = Field::decode(field_bytes)?;
        values.push(field);
    }

    Ok((version, values))
}

pub fn get_primary_key_fields(record: &Record, schema: &Schema) -> Result<Vec<Field>, JoinError> {
    let mut key_fields = vec![];
    for key_index in schema.primary_index.iter() {
        let key_value = record
            .get_value(*key_index)
            .map_err(|e| JoinError::InvalidKey(record.to_owned(), e))?;

        key_fields.push(key_value.clone());
    }

    Ok(key_fields)
}

pub fn encode_lookup_key(version: Option<u32>, fields: &[Field]) -> Vec<u8> {
    let mut lookup_key = Vec::with_capacity(64);
    if let Some(version) = version {
        lookup_key.extend_from_slice(&version.to_be_bytes());
    } else {
        lookup_key.extend_from_slice(&[0_u8; 4]);
    }

    for field in fields.iter() {
        let field_bytes = field.encode();
        lookup_key.extend_from_slice(&(field_bytes.len() as u32).to_be_bytes());
        lookup_key.extend_from_slice(&field_bytes);
    }
    lookup_key
}
