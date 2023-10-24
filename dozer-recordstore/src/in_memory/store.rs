use std::{
    collections::{BTreeMap, HashMap},
    ops::DerefMut,
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
/// This struct maintains a one-to-one map from an increasing integer to a `RecordRef`.
/// It's essentially one `BTreeMap` with `record_pointer_to_index` and `next_index` as computed fields.
struct ProcessorRecordStoreInner {
    records: BTreeMap<usize, Weak<RecordRefInner>>,
    record_pointer_to_index: HashMap<usize, usize>,
    next_index: usize,
}

impl ProcessorRecordStore {
    pub fn new() -> Result<Self, RecordStoreError> {
        Ok(Self {
            inner: RwLock::new(Default::default()),
        })
    }

    pub fn num_records(&self) -> usize {
        self.inner.read().next_index
    }

    /// Returns the serialized data and the `start` for next `serialize_slice` call.
    pub fn serialize_slice(&self, start: usize) -> Result<(Vec<u8>, usize), RecordStoreError> {
        let inner = self.inner.read();
        let slice = inner
            .records
            .range(start..)
            .filter_map(|(&id, weak)| weak.upgrade().map(|record| (id, RecordRef(record))))
            .collect::<Vec<_>>();
        let data =
            bincode::serialize(&slice).map_err(|e| RecordStoreError::SerializationError {
                typ: "[(usize, RecordRef)]",
                reason: Box::new(e),
            })?;
        Ok((data, inner.next_index))
    }

    pub fn serialize_ref(&self, record_ref: &RecordRef) -> u64 {
        *self
            .inner
            .read()
            .record_pointer_to_index
            .get(&(record_ref.id()))
            .expect("RecordRef not found in ProcessorRecordStore") as u64
    }

    pub fn vacuum(&self) {
        let mut inner = self.inner.write();
        let inner = inner.deref_mut();
        let ptr_to_idx = &mut inner.record_pointer_to_index;
        let records = &mut inner.records;

        records.retain(|_, record_ref| {
            if record_ref.strong_count() == 0 {
                ptr_to_idx.remove(&(record_ref.as_ptr() as usize));
                false
            } else {
                true
            }
        });
    }
}

impl StoreRecord for ProcessorRecordStore {
    fn store_record(&self, record: &RecordRef) -> Result<(), RecordStoreError> {
        let mut inner = self.inner.write();
        let inner = inner.deref_mut();

        inner
            .records
            .insert(inner.next_index, Arc::downgrade(&record.0));
        insert_record_pointer_to_index(
            &mut inner.record_pointer_to_index,
            record,
            inner.next_index,
        );
        inner.next_index += 1;

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
    records: BTreeMap<usize, RecordRef>,
    record_pointer_to_index: HashMap<usize, usize>,
}

impl ProcessorRecordStoreDeserializer {
    pub fn new() -> Result<Self, RecordStoreError> {
        Ok(Self {
            inner: RwLock::new(ProcessorRecordStoreDeserializerInner {
                records: BTreeMap::new(),
                record_pointer_to_index: HashMap::new(),
            }),
        })
    }

    pub fn deserialize_and_extend(&self, data: &[u8]) -> Result<(), RecordStoreError> {
        let slice: Vec<(usize, RecordRef)> =
            bincode::deserialize(data).map_err(|e| RecordStoreError::DeserializationError {
                typ: "[(usize, RecordRef)]",
                reason: Box::new(e),
            })?;

        let mut inner = self.inner.write();

        for (idx, record) in &slice {
            insert_record_pointer_to_index(&mut inner.record_pointer_to_index, record, *idx);
        }

        inner.records.extend(slice);

        Ok(())
    }

    pub fn deserialize_ref(&self, index: u64) -> Result<RecordRef, RecordStoreError> {
        Ok(self
            .inner
            .read()
            .records
            .get(&(index as usize))
            .ok_or(RecordStoreError::InMemoryRecordNotFound(index))?
            .clone())
    }

    pub fn into_record_store(self) -> ProcessorRecordStore {
        let inner = self.inner.into_inner();
        let next_index = inner
            .records
            .last_key_value()
            .map(|(index, _)| index + 1)
            .unwrap_or(0);
        ProcessorRecordStore {
            inner: RwLock::new(ProcessorRecordStoreInner {
                next_index,
                records: inner
                    .records
                    .into_iter()
                    .map(|(idx, record)| (idx, Arc::downgrade(&record.0)))
                    .collect(),
                record_pointer_to_index: inner.record_pointer_to_index,
            }),
        }
    }
}

impl StoreRecord for ProcessorRecordStoreDeserializer {
    fn store_record(&self, record: &RecordRef) -> Result<(), RecordStoreError> {
        let mut inner = self.inner.write();

        let idx = inner
            .records
            .last_key_value()
            .map(|(index, _)| index + 1)
            .unwrap_or(0);
        insert_record_pointer_to_index(&mut inner.record_pointer_to_index, record, idx);
        inner.records.insert(idx, record.clone());

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
            deserialized_record_refs
                .push(record_store.deserialize_ref(serialized_record_ref).unwrap());
        }
        assert_eq!(deserialized_record_refs, record_refs);
    }

    #[test]
    fn test_multiple_serialization_calls() {
        let record_store = ProcessorRecordStore::new().unwrap();
        record_store.create_ref(&[Field::Int(0)]).unwrap();
        record_store.create_ref(&[Field::Int(1)]).unwrap();
        let record1 = record_store.create_ref(&[Field::Int(2)]).unwrap();
        record_store.vacuum();
        let persisted1 = record_store.serialize_ref(&record1);
        assert_eq!(persisted1, 2);
        let (data1, next_index) = record_store.serialize_slice(0).unwrap();
        assert_eq!(next_index, 3);
        let record2 = record_store.create_ref(&[Field::Int(3)]).unwrap();
        let persisted2 = record_store.serialize_ref(&record2);
        assert_eq!(persisted2, 3);
        let (data2, next_index) = record_store.serialize_slice(next_index).unwrap();
        assert_eq!(next_index, 4);

        let record_store = ProcessorRecordStoreDeserializer::new().unwrap();
        record_store.deserialize_and_extend(&data1).unwrap();
        record_store.deserialize_and_extend(&data2).unwrap();
        let deserialized1 = record_store.deserialize_ref(persisted1).unwrap();
        assert_eq!(deserialized1, record1);
        let deserialized2 = record_store.deserialize_ref(persisted2).unwrap();
        assert_eq!(deserialized2, record2);

        let record_store = record_store.into_record_store();
        assert_eq!(
            record_store.serialize_ref(&record_store.create_ref(&[]).unwrap()),
            next_index as u64
        );
    }
}
