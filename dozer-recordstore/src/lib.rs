use dozer_types::{
    bincode,
    errors::internal::BoxedError,
    models::app_config::RecordStore,
    serde::{Deserialize, Serialize},
    thiserror::{self, Error},
    types::{Field, Lifetime, Record},
};
use in_memory::{FieldRef, StoreRecord as _};

#[derive(Error, Debug)]
pub enum RecordStoreError {
    #[error("Unable to deserialize type: {} - Reason: {}", typ, reason.to_string())]
    DeserializationError {
        typ: &'static str,
        reason: BoxedError,
    },

    #[error("Unable to serialize type: {} - Reason: {}", typ, reason.to_string())]
    SerializationError {
        typ: &'static str,
        reason: BoxedError,
    },
    #[error("Failed to create tempdir: {0}")]
    FailedToCreateTempDir(#[source] std::io::Error),
    #[error("Storage error: {0}")]
    Storage(#[from] dozer_storage::errors::StorageError),
    #[error("In memory record not found {0}")]
    InMemoryRecordNotFound(u64),
    #[error("Rocksdb record not found: {0}")]
    RocksdbRecordNotFound(u64),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RecordRef {
    InMemory(in_memory::RecordRef),
    Rocksdb(u64),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ProcessorRecord {
    /// All `Field`s in this record. The `Field`s are grouped by `Arc` to reduce memory usage.
    /// This is a Box<[]> instead of a Vec to save space on storing the vec's capacity
    values: Box<[RecordRef]>,

    /// Time To Live for this record. If the value is None, the record will never expire.
    lifetime: Option<Box<Lifetime>>,
}

impl ProcessorRecord {
    pub fn new(values: Box<[RecordRef]>) -> Self {
        Self {
            values,
            ..Default::default()
        }
    }

    pub fn get_lifetime(&self) -> Option<Lifetime> {
        self.lifetime.as_ref().map(|lifetime| *lifetime.clone())
    }
    pub fn set_lifetime(&mut self, lifetime: Option<Lifetime>) {
        self.lifetime = lifetime.map(Box::new);
    }

    pub fn values(&self) -> &[RecordRef] {
        &self.values
    }

    pub fn appended(existing: &ProcessorRecord, additional: RecordRef) -> Self {
        let mut values = Vec::with_capacity(existing.values().len() + 1);
        values.extend_from_slice(existing.values());
        values.push(additional);
        Self::new(values.into_boxed_slice())
    }
}

pub trait StoreRecord {
    fn create_ref(&self, values: &[Field]) -> Result<RecordRef, RecordStoreError>;

    fn load_ref(&self, record_ref: &RecordRef) -> Result<Vec<Field>, RecordStoreError>;

    fn create_record(&self, record: &Record) -> Result<ProcessorRecord, RecordStoreError> {
        let record_ref = self.create_ref(&record.values)?;
        let mut processor_record = ProcessorRecord::new(Box::new([record_ref]));
        processor_record.set_lifetime(record.lifetime.clone());
        Ok(processor_record)
    }

    fn load_record(&self, processor_record: &ProcessorRecord) -> Result<Record, RecordStoreError> {
        let mut record = Record::default();
        for record_ref in processor_record.values.iter() {
            let fields = self.load_ref(record_ref)?;
            record.values.extend(fields);
        }
        record.set_lifetime(processor_record.get_lifetime());
        Ok(record)
    }
}

#[derive(Debug)]
pub enum ProcessorRecordStore {
    InMemory(in_memory::ProcessorRecordStore),
    Rocksdb(rocksdb::ProcessorRecordStore),
}

impl ProcessorRecordStore {
    pub fn new(record_store: RecordStore) -> Result<Self, RecordStoreError> {
        match record_store {
            RecordStore::InMemory => Ok(Self::InMemory(in_memory::ProcessorRecordStore::new()?)),
            RecordStore::Rocksdb => Ok(Self::Rocksdb(rocksdb::ProcessorRecordStore::new()?)),
        }
    }

    pub fn num_records(&self) -> usize {
        match self {
            Self::InMemory(store) => store.num_records(),
            Self::Rocksdb(store) => store.num_records(),
        }
    }

    pub fn serialize_slice(&self, start: usize) -> Result<(Vec<u8>, usize), RecordStoreError> {
        match self {
            Self::InMemory(store) => store.serialize_slice(start),
            Self::Rocksdb(store) => store.serialize_slice(start),
        }
    }

    pub fn serialize_record(&self, record: &ProcessorRecord) -> Result<Vec<u8>, bincode::Error> {
        let ProcessorRecord { values, lifetime } = record;
        let values = values
            .iter()
            .map(|value| match (value, self) {
                (RecordRef::InMemory(record_ref), ProcessorRecordStore::InMemory(record_store)) => {
                    record_store.serialize_ref(record_ref)
                }
                (RecordRef::Rocksdb(record_ref), _) => *record_ref,
                _ => panic!("In memory record ref cannot be serialized by rocksdb record store"),
            })
            .collect();
        let record = ProcessorRecordForSerialization {
            values,
            lifetime: lifetime.clone(),
        };
        bincode::serialize(&record)
    }
}

impl StoreRecord for ProcessorRecordStore {
    fn create_ref(&self, values: &[Field]) -> Result<RecordRef, RecordStoreError> {
        match self {
            Self::InMemory(store) => Ok(RecordRef::InMemory(store.create_ref(values)?)),
            Self::Rocksdb(store) => Ok(RecordRef::Rocksdb(store.create_ref(values)?)),
        }
    }

    fn load_ref(&self, record_ref: &RecordRef) -> Result<Vec<Field>, RecordStoreError> {
        match (record_ref, self) {
            (RecordRef::InMemory(record_ref), _) => Ok(load_in_memory_record_ref(record_ref)),
            (RecordRef::Rocksdb(record_ref), ProcessorRecordStore::Rocksdb(record_store)) => {
                Ok(record_store.load_ref(record_ref)?)
            }
            _ => panic!("Rocksdb record ref cannot be loaded by in memory record store"),
        }
    }
}

#[derive(Debug)]
pub enum ProcessorRecordStoreDeserializer {
    InMemory(in_memory::ProcessorRecordStoreDeserializer),
    Rocksdb(rocksdb::ProcessorRecordStore),
}

impl ProcessorRecordStoreDeserializer {
    pub fn new(record_store: RecordStore) -> Result<Self, RecordStoreError> {
        match record_store {
            RecordStore::InMemory => Ok(Self::InMemory(
                in_memory::ProcessorRecordStoreDeserializer::new()?,
            )),
            RecordStore::Rocksdb => Ok(Self::Rocksdb(rocksdb::ProcessorRecordStore::new()?)),
        }
    }

    pub fn deserialize_and_extend(&self, data: &[u8]) -> Result<(), RecordStoreError> {
        match self {
            Self::InMemory(store) => store.deserialize_and_extend(data),
            Self::Rocksdb(store) => store.deserialize_and_extend(data),
        }
    }

    pub fn deserialize_record(&self, data: &[u8]) -> Result<ProcessorRecord, RecordStoreError> {
        let ProcessorRecordForSerialization { values, lifetime } = bincode::deserialize(data)?;
        let mut deserialized_values = Vec::with_capacity(values.len());
        for value in values {
            match self {
                Self::InMemory(record_store) => {
                    let record_ref = record_store.deserialize_ref(value)?;
                    deserialized_values.push(RecordRef::InMemory(record_ref));
                }
                Self::Rocksdb(_) => deserialized_values.push(RecordRef::Rocksdb(value)),
            }
        }
        Ok(ProcessorRecord {
            values: deserialized_values.into(),
            lifetime,
        })
    }

    pub fn into_record_store(self) -> ProcessorRecordStore {
        match self {
            Self::InMemory(record_store) => {
                ProcessorRecordStore::InMemory(record_store.into_record_store())
            }
            Self::Rocksdb(record_store) => ProcessorRecordStore::Rocksdb(record_store),
        }
    }
}

impl StoreRecord for ProcessorRecordStoreDeserializer {
    fn create_ref(&self, values: &[Field]) -> Result<RecordRef, RecordStoreError> {
        match self {
            Self::InMemory(store) => Ok(RecordRef::InMemory(store.create_ref(values)?)),
            Self::Rocksdb(store) => Ok(RecordRef::Rocksdb(store.create_ref(values)?)),
        }
    }

    fn load_ref(&self, record_ref: &RecordRef) -> Result<Vec<Field>, RecordStoreError> {
        match (record_ref, self) {
            (RecordRef::InMemory(record_ref), _) => Ok(load_in_memory_record_ref(record_ref)),
            (
                RecordRef::Rocksdb(record_ref),
                ProcessorRecordStoreDeserializer::Rocksdb(record_store),
            ) => Ok(record_store.load_ref(record_ref)?),
            _ => panic!("Rocksdb record ref cannot be loaded by in memory record store"),
        }
    }
}

fn load_in_memory_record_ref(record_ref: &in_memory::RecordRef) -> Vec<Field> {
    record_ref.load().iter().map(FieldRef::cloned).collect()
}

mod in_memory;
mod rocksdb;

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

    fn test_record_roundtrip_impl(record_store_kind: RecordStore) {
        let record = test_record();
        let record_store = ProcessorRecordStore::new(record_store_kind).unwrap();
        let processor_record = record_store.create_record(&record).unwrap();
        assert_eq!(record_store.load_record(&processor_record).unwrap(), record);
    }

    #[test]
    fn test_record_roundtrip() {
        test_record_roundtrip_impl(RecordStore::InMemory);
        test_record_roundtrip_impl(RecordStore::Rocksdb);
    }

    fn test_record_serialization_roundtrip_impl(record_store_kind: RecordStore) {
        let record_store = ProcessorRecordStore::new(record_store_kind).unwrap();
        let record = record_store.create_record(&test_record()).unwrap();
        let serialized_record = record_store.serialize_record(&record).unwrap();
        let data = record_store.serialize_slice(0).unwrap().0;

        let record_store = ProcessorRecordStoreDeserializer::new(record_store_kind).unwrap();
        record_store.deserialize_and_extend(&data).unwrap();
        let deserialized_record = record_store.deserialize_record(&serialized_record).unwrap();
        assert_eq!(deserialized_record, record);
    }

    #[test]
    fn test_record_serialization_roundtrip() {
        test_record_serialization_roundtrip_impl(RecordStore::InMemory);
        // TODO: enable this test when serialization is implemented for rocksdb
        // test_record_serialization_roundtrip_impl(RecordStore::Rocksdb);
    }
}
