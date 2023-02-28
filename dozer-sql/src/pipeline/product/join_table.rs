use std::collections::HashMap;

use dozer_core::{node::PortHandle, record_store::RecordReader};
use dozer_types::{
    errors::types::DeserializationError,
    types::{Field, Record, Schema},
};

use crate::pipeline::errors::JoinError;

use super::join::JoinAction;

#[derive(Clone, Debug)]
pub struct JoinTable {
    port: PortHandle,

    pub schema: Schema,
}

impl JoinTable {
    pub fn new(port: PortHandle, schema: Schema) -> Self {
        Self { port, schema }
    }

    pub fn get_source(&self) -> PortHandle {
        self.port
    }

    pub fn execute(
        &self,
        action: JoinAction,
        from_port: PortHandle,
        record: &Record,
    ) -> Result<Vec<(JoinAction, Record, Vec<u8>)>, JoinError> {
        debug_assert!(self.port == from_port);

        if self.schema.primary_index.is_empty() {
            let lookup_key = self.encode_record(record);
            Ok(vec![(action, record.clone(), lookup_key)])
        } else {
            let lookup_key = self.encode_lookup_key(record, &self.schema)?;
            Ok(vec![(action, record.clone(), lookup_key)])
        }
    }

    pub fn lookup(
        &self,
        lookup_key: &[u8],
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(Record, Vec<u8>)>, JoinError> {
        if self.schema.primary_index.is_empty() {
            let record = self
                .decode_record(lookup_key)
                .map_err(JoinError::DeserializationError)?;
            return Ok(vec![(record, lookup_key.to_vec())]);
        }

        let reader = readers
            .get(&self.port)
            .ok_or(JoinError::HistoryUnavailable(self.port))?;

        let (version, id) = self.get_lookup_key_version(lookup_key);

        let mut output_records = vec![];
        if let Some(record) = reader
            .get(&id, version)
            .map_err(|err| JoinError::HistoryRecordNotFound(id, version, self.port, err))?
        {
            output_records.push((record, lookup_key.to_vec()));
        }
        Ok(output_records)
    }

    // Encode the Fields of the Primary Key into a byte array
    // The byte array is used as the lookup key for the JoinTable
    //
    // Format:
    // 4 bytes: version
    // 4 bytes: field 1 length
    // n bytes: field 1 bytes
    // 4 bytes: field 2 length
    // n bytes: field 2 bytes
    // ...
    // 4 bytes: field n length
    // n bytes: field n bytes
    fn encode_lookup_key(&self, record: &Record, schema: &Schema) -> Result<Vec<u8>, JoinError> {
        let mut lookup_key = Vec::with_capacity(64);
        if let Some(version) = record.version {
            lookup_key.extend_from_slice(&version.to_be_bytes());
        } else {
            lookup_key.extend_from_slice(&[0_u8; 4]);
        }

        for key_index in schema.primary_index.iter() {
            let key_value = record
                .get_value(*key_index)
                .map_err(|e| JoinError::InvalidKey(record.to_owned(), e))?;

            let key_bytes = key_value.encode();
            lookup_key.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            lookup_key.extend_from_slice(&key_bytes);
        }

        Ok(lookup_key)
    }

    fn get_lookup_key_version(&self, lookup_key: &[u8]) -> (u32, Vec<u8>) {
        let (version_bytes, key) = lookup_key.split_at(4);
        let (_, id) = key.split_at(4);
        let version = u32::from_be_bytes(version_bytes.try_into().unwrap());
        (version, id.to_vec())
    }

    fn encode_record(&self, record: &Record) -> Vec<u8> {
        let mut record_bytes = Vec::with_capacity(64);
        if let Some(version) = record.version {
            record_bytes.extend_from_slice(&version.to_be_bytes());
        } else {
            record_bytes.extend_from_slice(&[0_u8; 4]);
        }

        for value in record.values.iter() {
            let value_bytes = value.encode();
            record_bytes.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
            record_bytes.extend_from_slice(&value_bytes);
        }
        record_bytes
    }

    fn decode_record(&self, record_bytes: &[u8]) -> Result<Record, DeserializationError> {
        let mut offset = 0;

        let record_version = u32::from_be_bytes([
            record_bytes[offset],
            record_bytes[offset + 1],
            record_bytes[offset + 2],
            record_bytes[offset + 3],
        ]);
        offset += 4;

        let version = if record_version != 0 {
            Some(record_version)
        } else {
            None
        };

        let mut values = vec![];
        while offset < record_bytes.len() {
            let field_length = u32::from_be_bytes([
                record_bytes[offset],
                record_bytes[offset + 1],
                record_bytes[offset + 2],
                record_bytes[offset + 3],
            ]);
            offset += 4;
            let field_bytes = &record_bytes[offset..offset + field_length as usize];
            let value = Field::decode(field_bytes)?;
            values.push(value);
            offset += field_length as usize;
        }
        Ok(Record::new(None, values, version))
    }
}
