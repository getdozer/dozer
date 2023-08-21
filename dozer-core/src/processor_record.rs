use std::hash::Hash;
use std::sync::Arc;

use dozer_storage::errors::StorageError;
use dozer_types::{
    bincode,
    parking_lot::RwLock,
    serde::{Deserialize, Serialize},
    types::{Field, Lifetime, Operation, Record},
};

use crate::{errors::ExecutionError, executor_operation::ProcessorOperation};

#[derive(Debug)]
pub struct ProcessorRecordStore {
    records: RwLock<Vec<RecordRef>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct RecordRef(Arc<[Field]>);

impl ProcessorRecordStore {
    pub fn new() -> Result<Self, ExecutionError> {
        Ok(Self {
            records: RwLock::new(vec![]),
        })
    }

    pub fn num_records(&self) -> usize {
        self.records.read().len()
    }

    pub fn serialize_slice(&self, start: usize) -> Result<(Vec<u8>, usize), StorageError> {
        let records = self.records.read();
        let slice = &records[start..];
        let data = bincode::serialize(slice).map_err(|e| StorageError::SerializationError {
            typ: "[RecordRef]",
            reason: Box::new(e),
        })?;
        Ok((data, slice.len()))
    }

    pub fn create_ref(&self, values: &[Field]) -> Result<RecordRef, StorageError> {
        let record = RecordRef(values.to_vec().into());
        self.records.write().push(record.clone());
        Ok(record)
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
        self.values.pop()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use dozer_types::types::Timestamp;

    use super::*;

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

        let record_store = ProcessorRecordStore::new().unwrap();
        let processor_record = record_store.create_record(&record).unwrap();
        assert_eq!(record_store.load_record(&processor_record).unwrap(), record);
    }
}
