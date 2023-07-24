use std::hash::Hash;
use std::sync::Arc;

use dozer_types::types::{Field, Lifetime, Record, Schema};

#[derive(Debug, PartialEq, Eq, Hash, Default)]
pub struct ProcessorRecord {
    /// Every element of this `Vec` is either a referenced `ProcessorRecord` (can be nested recursively) or a direct field.
    values: Vec<RefOrField>,
    /// This is a cache of sum of number of fields in `values` recursively. Must be kept consistent with `values`.
    total_len: u32,

    /// Time To Live for this record. If the value is None, the record will never expire.
    pub lifetime: Option<Lifetime>,

    // Imagine that we flatten all the fields in `values` recursively, `index` is the index into the flattened vector.
    index: Vec<u32>,
}

#[derive(Debug, PartialEq, Eq, Hash)]

enum RefOrField {
    Ref(ProcessorRecordRef),
    Field(Field),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProcessorRecordRef(Arc<ProcessorRecord>);

impl ProcessorRecordRef {
    pub fn new(record: ProcessorRecord) -> Self {
        ProcessorRecordRef(Arc::new(record))
    }

    pub fn get_record(&self) -> &ProcessorRecord {
        &self.0
    }
}

impl From<Record> for ProcessorRecord {
    fn from(record: Record) -> Self {
        let mut ref_record = ProcessorRecord::new();
        for field in record.values {
            ref_record.extend_direct_field(field);
        }
        ref_record
    }
}

impl ProcessorRecord {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_referenced_record(record: ProcessorRecordRef) -> Self {
        let mut result = ProcessorRecord::new();
        result.extend_referenced_record(record);
        result
    }

    pub fn clone_deref(&self) -> Record {
        let mut values: Vec<Field> = Vec::new();
        for field in self.get_fields() {
            values.push(field.clone());
        }
        let mut record = Record::new(values);
        record.set_lifetime(self.get_lifetime());
        record
    }

    pub fn get_lifetime(&self) -> Option<Lifetime> {
        self.lifetime.clone()
    }
    pub fn set_lifetime(&mut self, lifetime: Option<Lifetime>) {
        self.lifetime = lifetime;
    }

    pub fn get_field_indexes(&self) -> &[u32] {
        &self.index
    }

    pub fn extend_referenced_fields(
        &mut self,
        other: ProcessorRecordRef,
        field_indexes: impl IntoIterator<Item = u32>,
    ) {
        for idx in field_indexes {
            self.index.push(self.total_len + idx);
        }

        self.total_len += other.get_record().total_len;

        self.values.push(RefOrField::Ref(other));
    }

    pub fn extend_referenced_record(&mut self, other: ProcessorRecordRef) {
        for index in other.get_record().get_field_indexes() {
            self.index.push(self.total_len + index);
        }

        self.total_len += other.get_record().total_len;

        self.values.push(RefOrField::Ref(other));
    }

    pub fn extend_direct_field(&mut self, field: Field) {
        self.values.push(RefOrField::Field(field));
        self.index.push(self.total_len);
        self.total_len += 1;
    }

    pub fn get_fields(&self) -> Vec<&Field> {
        let mut fields = Vec::new();
        for idx in &self.index {
            let field = self.get_field_by_index(*idx);

            fields.push(field);
        }
        fields
    }

    // Function to get a field by its index
    pub fn get_field_by_index(&self, index: u32) -> &Field {
        let mut current_index = index;

        // Iterate through the values and update the counts
        for field_or_ref in self.values.iter() {
            match field_or_ref {
                RefOrField::Ref(record_ref) => {
                    // If it's a reference, check if it matches the given index
                    let rec = record_ref.get_record();
                    let count = rec.total_len;
                    if current_index < count {
                        return rec.get_field_by_index(current_index);
                    }
                    current_index -= count;
                }
                RefOrField::Field(field) => {
                    // If it's a field, check if it matches the given index
                    if current_index == 0 {
                        return field;
                    }
                    current_index -= 1;
                }
            }
        }

        panic!("Index {index} out of range {}", self.total_len);
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
            values: (0..size).map(|_| RefOrField::Field(Field::Null)).collect(),
            total_len: size as u32,
            lifetime: None,
            index: (0..size as u32).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_record_nulls() {
        let record = ProcessorRecord::nulls(3);
        assert_eq!(record.get_field_indexes(), &[0, 1, 2]);
        assert_eq!(record.get_fields().len(), 3);
        assert_eq!(record.get_field_by_index(0), &Field::Null);
        assert_eq!(record.get_field_by_index(1), &Field::Null);
        assert_eq!(record.get_field_by_index(2), &Field::Null);
    }

    #[test]
    fn test_processor_record_extend_direct_field() {
        let mut record = ProcessorRecord::new();
        record.extend_direct_field(Field::Int(1));

        assert_eq!(record.get_field_indexes(), &[0]);
        assert_eq!(record.get_fields().len(), 1);
        assert_eq!(record.get_field_by_index(0), &Field::Int(1));
    }

    #[test]
    fn test_processor_record_extend_referenced_record() {
        let mut record = ProcessorRecord::new();
        let mut other = ProcessorRecord::new();
        other.extend_direct_field(Field::Int(1));
        other.extend_direct_field(Field::Int(2));
        record.extend_referenced_record(ProcessorRecordRef::new(other));

        assert_eq!(record.get_field_indexes(), &[0, 1]);
        assert_eq!(record.get_fields().len(), 2);
        assert_eq!(record.get_field_by_index(0), &Field::Int(1));
        assert_eq!(record.get_field_by_index(1), &Field::Int(2));
    }

    #[test]
    fn test_processor_record_extend_referenced_fields() {
        let mut record = ProcessorRecord::new();
        let mut other = ProcessorRecord::new();
        other.extend_direct_field(Field::Int(1));
        other.extend_direct_field(Field::Int(2));
        record.extend_referenced_fields(ProcessorRecordRef::new(other), vec![1]);

        assert_eq!(record.get_field_indexes(), &[1]);
        assert_eq!(record.get_fields().len(), 1);
        assert_eq!(record.get_field_by_index(1), &Field::Int(2));
    }

    #[test]
    fn test_processor_record_extend_interleave() {
        let mut record = ProcessorRecord::new();
        let mut other = ProcessorRecord::new();
        other.extend_direct_field(Field::Int(1));
        other.extend_direct_field(Field::Int(2));
        let other = ProcessorRecordRef::new(other);
        record.extend_direct_field(Field::Int(3));
        record.extend_referenced_record(other.clone());
        record.extend_direct_field(Field::Int(4));
        record.extend_referenced_fields(other, vec![1]);

        assert_eq!(record.get_field_indexes(), &[0, 1, 2, 3, 5]);
        assert_eq!(record.get_fields().len(), 5);
        assert_eq!(record.get_field_by_index(0), &Field::Int(3));
        assert_eq!(record.get_field_by_index(1), &Field::Int(1));
        assert_eq!(record.get_field_by_index(2), &Field::Int(2));
        assert_eq!(record.get_field_by_index(3), &Field::Int(4));
        assert_eq!(record.get_field_by_index(5), &Field::Int(2));
    }

    #[test]
    fn test_processor_record_extend_nested() {
        let mut nested_inner = ProcessorRecord::new();
        nested_inner.extend_direct_field(Field::Int(1));
        nested_inner.extend_direct_field(Field::Int(2));
        let nested = ProcessorRecordRef::new(nested_inner);

        let mut nested_outer = ProcessorRecord::new();
        nested_outer.extend_direct_field(Field::Int(3));
        nested_outer.extend_referenced_record(nested.clone());
        nested_outer.extend_direct_field(Field::Int(4));
        let nested_outer = ProcessorRecordRef::new(nested_outer);

        let mut record = ProcessorRecord::new();
        record.extend_direct_field(Field::Int(5));
        record.extend_referenced_record(nested_outer);
        record.extend_direct_field(Field::Int(6));

        assert_eq!(record.get_field_indexes(), &[0, 1, 2, 3, 4, 5]);
        assert_eq!(record.get_fields().len(), 6);
        assert_eq!(record.get_field_by_index(0), &Field::Int(5));
        assert_eq!(record.get_field_by_index(1), &Field::Int(3));
        assert_eq!(record.get_field_by_index(2), &Field::Int(1));
        assert_eq!(record.get_field_by_index(3), &Field::Int(2));
        assert_eq!(record.get_field_by_index(4), &Field::Int(4));
        assert_eq!(record.get_field_by_index(5), &Field::Int(6));
    }
}
