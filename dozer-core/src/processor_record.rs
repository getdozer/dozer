use std::hash::Hash;
use std::sync::Arc;

use dozer_storage::errors::StorageError;
use dozer_types::types::{Field, Lifetime, Record, Schema};

pub struct ProcessorRecordStore;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecordRef(Arc<Vec<Field>>);

impl ProcessorRecordStore {
    pub fn create_ref(values: &[Field]) -> Result<RecordRef, StorageError> {
        Ok(RecordRef(Arc::new(values.to_vec())))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ProcessorRecord {
    /// All `Field`s in this record. The `Field`s are grouped by `Arc` to reduce memory usage.
    values: Vec<Arc<Vec<Field>>>,

    /// Time To Live for this record. If the value is None, the record will never expire.
    lifetime: Option<Box<Lifetime>>,
}

impl From<Record> for ProcessorRecord {
    fn from(record: Record) -> Self {
        let mut ref_record = ProcessorRecord::new();
        ref_record.push(Arc::new(record.values));
        ref_record.set_lifetime(record.lifetime);
        ref_record
    }
}

impl ProcessorRecord {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clone_deref(&self) -> Record {
        let mut record = Record::new(self.get_fields().cloned().collect());
        record.set_lifetime(self.get_lifetime());
        record
    }

    pub fn get_lifetime(&self) -> Option<Lifetime> {
        self.lifetime.as_ref().map(|lifetime| *lifetime.clone())
    }
    pub fn set_lifetime(&mut self, lifetime: Option<Lifetime>) {
        self.lifetime = lifetime.map(Box::new);
    }

    pub fn extend(&mut self, other: ProcessorRecord) {
        self.values.extend(other.values);
    }

    pub fn push(&mut self, fields: Arc<Vec<Field>>) {
        self.values.push(fields);
    }

    pub fn pop(&mut self) -> Option<Arc<Vec<Field>>> {
        let result = self.values.pop();
        result
    }

    pub fn get_fields(&self) -> impl Iterator<Item = &Field> {
        self.values.iter().flat_map(|x| x.iter())
    }

    // Function to get a field by its index
    pub fn get_field_by_index(&self, index: u32) -> &Field {
        let mut current_index = index;

        // Iterate through the values and update the counts
        for fields in self.values.iter() {
            if current_index < fields.len() as u32 {
                return &fields[current_index as usize];
            } else {
                current_index -= fields.len() as u32;
            }
        }

        panic!("Index {index} out of range");
    }

    pub fn get_key(&self, indexes: &[usize]) -> Vec<u8> {
        debug_assert!(!indexes.is_empty(), "Primary key indexes cannot be empty");

        let mut tot_size = 0_usize;
        let mut buffers = Vec::<Vec<u8>>::with_capacity(indexes.len());
        for i in indexes {
            let bytes = self.get_field_by_index(*i as u32).encode();
            tot_size += bytes.len();
            buffers.push(bytes);
        }

        let mut res_buffer = Vec::<u8>::with_capacity(tot_size);
        for i in buffers {
            res_buffer.extend(i);
        }
        res_buffer
    }

    pub fn nulls_from_schema(schema: &Schema) -> ProcessorRecord {
        Self::nulls(schema.fields.len())
    }

    pub fn nulls(size: usize) -> ProcessorRecord {
        ProcessorRecord {
            values: vec![Arc::new((0..size).map(|_| Field::Null).collect())],
            lifetime: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use dozer_types::types::Timestamp;

    use super::*;

    #[test]
    fn test_processor_record_nulls() {
        let record = ProcessorRecord::nulls(3);
        assert_eq!(record.get_fields().count(), 3);
        assert_eq!(record.get_field_by_index(0), &Field::Null);
        assert_eq!(record.get_field_by_index(1), &Field::Null);
        assert_eq!(record.get_field_by_index(2), &Field::Null);
    }

    #[test]
    fn test_processor_record_extend_direct_field() {
        let mut record = ProcessorRecord::new();
        record.push(Arc::new(vec![Field::Int(1)]));

        assert_eq!(record.get_fields().count(), 1);
        assert_eq!(record.get_field_by_index(0), &Field::Int(1));
    }

    #[test]
    fn test_processor_record_extend_referenced_record() {
        let mut record = ProcessorRecord::new();
        let mut other = ProcessorRecord::new();
        other.push(Arc::new(vec![Field::Int(1), Field::Int(2)]));
        record.extend(other);

        assert_eq!(record.get_fields().count(), 2);
        assert_eq!(record.get_field_by_index(0), &Field::Int(1));
        assert_eq!(record.get_field_by_index(1), &Field::Int(2));
    }

    #[test]
    fn test_processor_record_extend_interleave() {
        let mut record = ProcessorRecord::new();
        let mut other = ProcessorRecord::new();
        other.push(Arc::new(vec![Field::Int(1), Field::Int(2)]));

        record.push(Arc::new(vec![Field::Int(3)]));
        record.extend(other);
        record.push(Arc::new(vec![Field::Int(4)]));

        assert_eq!(record.get_fields().count(), 4);
        assert_eq!(record.get_field_by_index(0), &Field::Int(3));
        assert_eq!(record.get_field_by_index(1), &Field::Int(1));
        assert_eq!(record.get_field_by_index(2), &Field::Int(2));
        assert_eq!(record.get_field_by_index(3), &Field::Int(4));
    }

    #[test]
    fn test_processor_record_extend_nested() {
        let mut nested_inner = ProcessorRecord::new();
        nested_inner.push(Arc::new(vec![Field::Int(1), Field::Int(2)]));

        let mut nested_outer = ProcessorRecord::new();
        nested_outer.push(Arc::new(vec![Field::Int(3)]));
        nested_outer.extend(nested_inner);
        nested_outer.push(Arc::new(vec![Field::Int(4)]));

        let mut record = ProcessorRecord::new();
        record.push(Arc::new(vec![Field::Int(5)]));
        record.extend(nested_outer);
        record.push(Arc::new(vec![Field::Int(6)]));

        assert_eq!(record.get_fields().count(), 6);
        assert_eq!(record.get_field_by_index(0), &Field::Int(5));
        assert_eq!(record.get_field_by_index(1), &Field::Int(3));
        assert_eq!(record.get_field_by_index(2), &Field::Int(1));
        assert_eq!(record.get_field_by_index(3), &Field::Int(2));
        assert_eq!(record.get_field_by_index(4), &Field::Int(4));
        assert_eq!(record.get_field_by_index(5), &Field::Int(6));
    }

    #[test]
    fn test_record_roundtrip() {
        let mut record = Record::new(vec![
            Field::Int(1),
            Field::Int(2),
            Field::Int(3),
            Field::Int(4),
        ]);
        record.lifetime = Some(Lifetime {
            reference: Timestamp::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap(),
            duration: Duration::from_secs(10),
        });

        let processor_record = ProcessorRecord::from(record.clone());
        assert_eq!(processor_record.clone_deref(), record);
    }
}
