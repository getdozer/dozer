use std::collections::HashMap;

use dozer_storage::errors::StorageError;
use dozer_types::{
    bincode,
    parking_lot::RwLock,
    serde::{Deserialize, Serialize},
    types::{Field, Lifetime, Operation, Record},
};

use crate::{errors::ExecutionError, executor_operation::ProcessorOperation};

use super::{ProcessorRecord, RecordRef};

#[derive(Debug)]
pub struct ProcessorRecordStore {
    inner: RwLock<ProcessorRecordStoreInner>,
}

#[derive(Debug, Default)]
struct ProcessorRecordStoreInner {
    records: Vec<RecordRef>,
    record_pointer_to_index: HashMap<usize, usize>,
}

impl ProcessorRecordStore {
    pub fn new() -> Result<Self, ExecutionError> {
        Ok(Self {
            inner: RwLock::new(Default::default()),
        })
    }

    pub fn num_records(&self) -> usize {
        self.inner.read().records.len()
    }

    pub fn serialize_slice(&self, start: usize) -> Result<(Vec<u8>, usize), StorageError> {
        let records = &self.inner.read().records;
        let slice = &records[start..];
        let data = bincode::serialize(slice).map_err(|e| StorageError::SerializationError {
            typ: "[RecordRef]",
            reason: Box::new(e),
        })?;
        Ok((data, slice.len()))
    }

    pub fn deserialize_and_extend(&self, data: &[u8]) -> Result<(), StorageError> {
        let slice: Vec<RecordRef> =
            bincode::deserialize(data).map_err(|e| StorageError::DeserializationError {
                typ: "[RecordRef]",
                reason: Box::new(e),
            })?;

        let mut inner = self.inner.write();

        let mut index = inner.records.len();
        for record in &slice {
            insert_record_pointer_to_index(&mut inner.record_pointer_to_index, record, index);
            index += 1;
        }

        inner.records.extend(slice);

        Ok(())
    }

    pub fn create_ref(&self, values: &[Field]) -> Result<RecordRef, StorageError> {
        let record = RecordRef(values.to_vec().into());

        let mut inner = self.inner.write();

        let index = inner.records.len();
        insert_record_pointer_to_index(&mut inner.record_pointer_to_index, &record, index);
        inner.records.push(record.clone());

        Ok(record)
    }

    pub fn load_ref(&self, record_ref: &RecordRef) -> Result<Vec<Field>, StorageError> {
        Ok(record_ref.0.to_vec())
    }

    pub fn serialize_ref(&self, record_ref: &RecordRef) -> u64 {
        *self
            .inner
            .read()
            .record_pointer_to_index
            .get(&(record_ref.0.as_ptr() as usize))
            .expect("RecordRef not found in ProcessorRecordStore") as u64
    }

    pub fn deserialize_ref(&self, index: u64) -> RecordRef {
        self.inner.read().records[index as usize].clone()
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

    pub fn serialize_record(&self, record: &ProcessorRecord) -> Result<Vec<u8>, bincode::Error> {
        let ProcessorRecord { values, lifetime } = record;
        let values = values
            .iter()
            .map(|value| self.serialize_ref(value))
            .collect();
        let record = ProcessorRecordForSerialization {
            values,
            lifetime: lifetime.clone(),
        };
        bincode::serialize(&record)
    }

    pub fn deserialize_record(&self, data: &[u8]) -> Result<ProcessorRecord, bincode::Error> {
        let ProcessorRecordForSerialization { values, lifetime } = bincode::deserialize(data)?;
        let values = values
            .iter()
            .map(|index| self.deserialize_ref(*index))
            .collect();
        Ok(ProcessorRecord { values, lifetime })
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

fn insert_record_pointer_to_index(
    record_pointer_to_index: &mut HashMap<usize, usize>,
    record: &RecordRef,
    index: usize,
) {
    let previous_index = record_pointer_to_index.insert(record.0.as_ptr() as usize, index);
    debug_assert!(previous_index.is_none());
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
struct ProcessorRecordForSerialization {
    values: Vec<u64>,
    lifetime: Option<Box<Lifetime>>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use dozer_types::types::Timestamp;

    use super::*;

    fn test_record() -> Record {
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
        record
    }

    #[test]
    fn test_record_roundtrip() {
        let record = test_record();
        let record_store = ProcessorRecordStore::new().unwrap();
        let processor_record = record_store.create_record(&record).unwrap();
        assert_eq!(record_store.load_record(&processor_record).unwrap(), record);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let record_store = ProcessorRecordStore::new().unwrap();
        let value_vecs = vec![
            vec![Field::Int(1), Field::Null],
            vec![Field::Int(2), Field::Null],
        ];

        let mut record_refs = vec![];
        for value in value_vecs {
            let record_ref = record_store.create_ref(&value).unwrap();
            record_refs.push(record_ref);
        }

        let mut serialized_record_refs = vec![];
        let data = record_store.serialize_slice(0).unwrap().0;
        for record_ref in &record_refs {
            serialized_record_refs.push(record_store.serialize_ref(record_ref));
        }

        let record_store = ProcessorRecordStore::new().unwrap();
        record_store.deserialize_and_extend(&data).unwrap();
        let mut deserialized_record_refs = vec![];
        for serialized_record_ref in serialized_record_refs {
            deserialized_record_refs.push(record_store.deserialize_ref(serialized_record_ref));
        }
        assert_eq!(deserialized_record_refs, record_refs);
    }

    #[test]
    fn test_record_serialization_roundtrip() {
        let record_store = ProcessorRecordStore::new().unwrap();
        let record = record_store.create_record(&test_record()).unwrap();
        let serialized_record = record_store.serialize_record(&record).unwrap();
        let deserialized_record = record_store.deserialize_record(&serialized_record).unwrap();
        assert_eq!(deserialized_record, record);
    }
}
