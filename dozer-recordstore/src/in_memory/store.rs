use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

use dozer_types::{bincode, parking_lot::RwLock, types::Field};

use crate::RecordStoreError;

use super::{RecordRef, RecordRefInner};

pub trait StoreRecord {
    fn store_record(&self, record: &RecordRef) -> Result<(), RecordStoreError>;

    fn create_ref(&self, values: &[Field]) -> Result<RecordRef, RecordStoreError> {
        let record = RecordRef::new(values.to_vec());
        self.store_record(&record)?;
        Ok(record)
    }
}

#[derive(Debug)]
pub struct ProcessorRecordStore {
    inner: RwLock<ProcessorRecordStoreInner>,
}

#[derive(Debug, Default)]
struct ProcessorRecordStoreInner {
    records: Vec<Weak<RecordRefInner>>,
    record_pointer_to_index: HashMap<usize, usize>,
}

impl ProcessorRecordStore {
    pub fn new() -> Result<Self, RecordStoreError> {
        Ok(Self {
            inner: RwLock::new(Default::default()),
        })
    }

    pub fn num_records(&self) -> usize {
        self.inner.read().records.len()
    }

    pub fn serialize_slice(&self, start: usize) -> Result<(Vec<u8>, usize), RecordStoreError> {
        let records = &self.inner.read().records;
        let slice = records[start..]
            .iter()
            .map(|record| record.upgrade().map(RecordRef))
            .collect::<Vec<_>>();
        let data =
            bincode::serialize(&slice).map_err(|e| RecordStoreError::SerializationError {
                typ: "[Option<RecordRef>]",
                reason: Box::new(e),
            })?;
        Ok((data, slice.len()))
    }

    pub fn serialize_ref(&self, record_ref: &RecordRef) -> u64 {
        *self
            .inner
            .read()
            .record_pointer_to_index
            .get(&(record_ref.id()))
            .expect("RecordRef not found in ProcessorRecordStore") as u64
    }
}

impl StoreRecord for ProcessorRecordStore {
    fn store_record(&self, record: &RecordRef) -> Result<(), RecordStoreError> {
        let mut inner = self.inner.write();

        let index = inner.records.len();
        insert_record_pointer_to_index(&mut inner.record_pointer_to_index, record, index);
        inner.records.push(Arc::downgrade(&record.0));

        Ok(())
    }
}

/// This struct is supposed to be used during deserialize records from disk.
///
/// The only difference between this struct and `ProcessorRecordStore` is that this struct uses `Arc` instead of `Weak`.
#[derive(Debug)]
pub struct ProcessorRecordStoreDeserializer {
    inner: RwLock<ProcessorRecordStoreDeserializerInner>,
}

#[derive(Debug)]
struct ProcessorRecordStoreDeserializerInner {
    records: Vec<Option<RecordRef>>,
    record_pointer_to_index: HashMap<usize, usize>,
}

impl ProcessorRecordStoreDeserializer {
    pub fn new() -> Result<Self, RecordStoreError> {
        Ok(Self {
            inner: RwLock::new(ProcessorRecordStoreDeserializerInner {
                records: vec![],
                record_pointer_to_index: HashMap::new(),
            }),
        })
    }

    pub fn deserialize_and_extend(&self, data: &[u8]) -> Result<(), RecordStoreError> {
        let slice: Vec<Option<RecordRef>> =
            bincode::deserialize(data).map_err(|e| RecordStoreError::DeserializationError {
                typ: "[Option<RecordRef>]",
                reason: Box::new(e),
            })?;

        let mut inner = self.inner.write();

        let mut index = inner.records.len();
        for record in &slice {
            if let Some(record) = record {
                insert_record_pointer_to_index(&mut inner.record_pointer_to_index, record, index);
            }
            index += 1;
        }

        inner.records.extend(slice);

        Ok(())
    }

    pub fn deserialize_ref(&self, index: u64) -> RecordRef {
        self.inner.read().records[index as usize]
            .as_ref()
            .unwrap_or_else(|| {
                panic!("RecordRef {index} not found in ProcessorRecordStoreDeserializer")
            })
            .clone()
    }

    pub fn into_record_store(self) -> ProcessorRecordStore {
        let inner = self.inner.into_inner();
        ProcessorRecordStore {
            inner: RwLock::new(ProcessorRecordStoreInner {
                records: inner
                    .records
                    .into_iter()
                    .map(|record| {
                        record.map_or_else(
                            || Arc::downgrade(&RecordRef::new(vec![]).0),
                            |record| Arc::downgrade(&record.0),
                        )
                    })
                    .collect(),
                record_pointer_to_index: inner.record_pointer_to_index,
            }),
        }
    }
}

impl StoreRecord for ProcessorRecordStoreDeserializer {
    fn store_record(&self, record: &RecordRef) -> Result<(), RecordStoreError> {
        let mut inner = self.inner.write();

        let index = inner.records.len();
        insert_record_pointer_to_index(&mut inner.record_pointer_to_index, record, index);
        inner.records.push(Some(record.clone()));

        Ok(())
    }
}

fn insert_record_pointer_to_index(
    record_pointer_to_index: &mut HashMap<usize, usize>,
    record: &RecordRef,
    index: usize,
) {
    let previous_index = record_pointer_to_index.insert(record.id(), index);
    debug_assert!(previous_index.is_none());
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let record_store = ProcessorRecordStoreDeserializer::new().unwrap();
        record_store.deserialize_and_extend(&data).unwrap();
        let mut deserialized_record_refs = vec![];
        for serialized_record_ref in serialized_record_refs {
            deserialized_record_refs.push(record_store.deserialize_ref(serialized_record_ref));
        }
        assert_eq!(deserialized_record_refs, record_refs);
    }
}
