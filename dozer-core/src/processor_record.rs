use std::hash::Hash;
use std::sync::Arc;

use dozer_storage::errors::StorageError;
use dozer_types::types::{Field, Lifetime, Operation, Record};

use crate::{errors::ExecutionError, executor_operation::ProcessorOperation};

#[derive(Debug, Clone)]
pub struct ProcessorRecordStore;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecordRef(Arc<[Field]>);

impl ProcessorRecordStore {
    pub fn new() -> Result<Self, ExecutionError> {
        Ok(Self)
    }

    pub fn create_ref(&self, values: &[Field]) -> Result<RecordRef, StorageError> {
        Ok(RecordRef(values.to_vec().into()))
    }

    pub fn load_ref(&self, record_ref: &RecordRef) -> Result<Vec<Field>, StorageError> {
        Ok(record_ref.0.to_vec())
    }

    pub fn create_record(&self, record: &Record) -> Result<ProcessorRecord, StorageError> {
        let record_ref = self.create_ref(&record.values)?;
        let mut processor_record = ProcessorRecord::new();
        processor_record.push(record_ref);
        processor_record.set_lifetime(record.lifetime.clone());
        Ok(processor_record)
    }

    pub fn load_record(&self, processor_record: &ProcessorRecord) -> Result<Record, StorageError> {
        let mut record = Record::default();
        for record_ref in processor_record.values.iter() {
            let fields = self.load_ref(record_ref)?;
            record.values.extend(fields.iter().cloned());
        }
        record.set_lifetime(processor_record.get_lifetime());
        Ok(record)
    }

    pub fn create_operation(
        &self,
        operation: &Operation,
    ) -> Result<ProcessorOperation, StorageError> {
        match operation {
            Operation::Delete { old } => {
                let old = self.create_record(old)?;
                Ok(ProcessorOperation::Delete { old })
            }
            Operation::Insert { new } => {
                let new = self.create_record(new)?;
                Ok(ProcessorOperation::Insert { new })
            }
            Operation::Update { old, new } => {
                let old = self.create_record(old)?;
                let new = self.create_record(new)?;
                Ok(ProcessorOperation::Update { old, new })
            }
        }
    }

    pub fn load_operation(
        &self,
        operation: &ProcessorOperation,
    ) -> Result<Operation, StorageError> {
        match operation {
            ProcessorOperation::Delete { old } => {
                let old = self.load_record(old)?;
                Ok(Operation::Delete { old })
            }
            ProcessorOperation::Insert { new } => {
                let new = self.load_record(new)?;
                Ok(Operation::Insert { new })
            }
            ProcessorOperation::Update { old, new } => {
                let old = self.load_record(old)?;
                let new = self.load_record(new)?;
                Ok(Operation::Update { old, new })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ProcessorRecord {
    /// All `Field`s in this record. The `Field`s are grouped by `Arc` to reduce memory usage.
    values: Vec<RecordRef>,

    /// Time To Live for this record. If the value is None, the record will never expire.
    lifetime: Option<Box<Lifetime>>,
}

impl ProcessorRecord {
    pub fn new() -> Self {
        Self::default()
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

    pub fn push(&mut self, record_ref: RecordRef) {
        self.values.push(record_ref);
    }

    pub fn pop(&mut self) -> Option<RecordRef> {
        let result = self.values.pop();
        result
    }
}

#[cfg(test)]
mod tests {
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
}
