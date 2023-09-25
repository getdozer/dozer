use std::{
    collections::{
        hash_map::{self, Values},
        HashMap,
    },
    iter::{once, Flatten, Once},
};

use dozer_core::dozer_log::storage::Object;
use dozer_recordstore::{
    ProcessorRecord, ProcessorRecordStore, ProcessorRecordStoreDeserializer, StoreRecord,
};
use dozer_types::{
    chrono,
    types::{Field, Record, Schema, Timestamp},
};
use linked_hash_map::LinkedHashMap;

use crate::{
    errors::JoinError,
    utils::{
        record_hashtable_key::{get_record_hash, RecordKey},
        serialize::{
            deserialize_bincode, deserialize_record, deserialize_u64, serialize_bincode,
            serialize_record, serialize_u64, Cursor, DeserializationError, SerializationError,
        },
    },
};

pub type JoinKey = RecordKey;
type IndexKey = (JoinKey, u64); // (join_key, primary_key)

#[derive(Debug, Clone)]
pub struct JoinTable {
    join_key_indexes: Vec<usize>,
    primary_key_indexes: Vec<usize>,
    default_record: ProcessorRecord,
    map: HashMap<JoinKey, HashMap<u64, Vec<ProcessorRecord>>>,
    lifetime_map: LinkedHashMap<Timestamp, Vec<IndexKey>>,
    accurate_keys: bool,
}

impl JoinTable {
    pub fn new(
        schema: &Schema,
        join_key_indexes: Vec<usize>,
        record_store: &ProcessorRecordStoreDeserializer,
        accurate_keys: bool,
        cursor: Option<&mut Cursor>,
    ) -> Result<Self, JoinError> {
        let primary_key_indexes = if schema.primary_index.is_empty() {
            (0..schema.fields.len()).collect()
        } else {
            schema.primary_index.clone()
        };

        let (default_record, map, lifetime_map) = if let Some(cursor) = cursor {
            (
                deserialize_record(cursor, record_store)?,
                deserialize_join_map(cursor, record_store)?,
                deserialize_bincode(cursor)?,
            )
        } else {
            (
                record_store.create_record(&Record::nulls_from_schema(schema))?,
                Default::default(),
                Default::default(),
            )
        };
        Ok(Self {
            join_key_indexes,
            primary_key_indexes,
            default_record,
            map,
            lifetime_map,
            accurate_keys,
        })
    }

    pub fn lookup_size(&self) -> usize {
        self.map.len()
    }

    pub fn get_matching_records<'a>(
        &'a self,
        join_key: &JoinKey,
        default_if_no_match: bool,
    ) -> MatchingRecords<'a> {
        if let Some(records_map) = self.map.get(join_key) {
            MatchingRecords::Values(records_map.values().flatten())
        } else if default_if_no_match {
            MatchingRecords::Default(once(&self.default_record))
        } else {
            MatchingRecords::Empty
        }
    }

    pub fn default_record(&self) -> &ProcessorRecord {
        &self.default_record
    }

    pub fn insert(
        &mut self,
        record: ProcessorRecord,
        record_decoded: &Record,
    ) -> Result<JoinKey, JoinError> {
        let join_key = self.get_join_key(record_decoded);
        let primary_key = get_record_key_hash(record_decoded, &self.primary_key_indexes);

        if let Some(lifetime) = record.get_lifetime() {
            let Some(eviction_instant) =
                lifetime
                    .reference
                    .checked_add_signed(chrono::Duration::nanoseconds(
                        lifetime.duration.as_nanos() as i64,
                    ))
            else {
                return Err(JoinError::EvictionTimeOverflow);
            };

            self.lifetime_map
                .entry(eviction_instant)
                .or_default()
                .push((join_key.clone(), primary_key));
        }

        self.map
            .entry(join_key.clone())
            .or_default()
            .entry(primary_key)
            .or_default()
            .push(record);

        Ok(join_key)
    }

    pub fn remove(&mut self, record: &Record) -> JoinKey {
        let join_key = self.get_join_key(record);
        if let hash_map::Entry::Occupied(record_map) = self.map.entry(join_key.clone()) {
            let primary_key = get_record_key_hash(record, &self.primary_key_indexes);
            remove_record_using_primary_key(record_map, primary_key);
        }
        join_key
    }

    pub fn evict_index(&mut self, now: &Timestamp) {
        let mut keys_to_remove = vec![];
        for (eviction_instant, join_index_keys) in self.lifetime_map.iter() {
            if eviction_instant <= now {
                keys_to_remove.push(*eviction_instant);
                for (join_key, primary_key) in join_index_keys {
                    if let hash_map::Entry::Occupied(record_map) = self.map.entry(join_key.clone())
                    {
                        remove_record_using_primary_key(record_map, *primary_key);
                    }
                }
            } else {
                break;
            }
        }

        for key in keys_to_remove {
            self.lifetime_map.remove(&key);
        }
    }

    pub fn serialize(
        &self,
        record_store: &ProcessorRecordStore,
        object: &mut Object,
    ) -> Result<(), SerializationError> {
        serialize_record(&self.default_record, record_store, object)?;
        serialize_join_map(&self.map, record_store, object)?;
        serialize_bincode(&self.lifetime_map, object)?;
        Ok(())
    }

    fn get_join_key(&self, record: &Record) -> JoinKey {
        if self.accurate_keys {
            JoinKey::Accurate(get_record_key_fields(record, &self.join_key_indexes))
        } else {
            JoinKey::Hash(get_record_key_hash(record, &self.join_key_indexes))
        }
    }
}

#[derive(Debug)]
pub enum MatchingRecords<'a> {
    Values(Flatten<Values<'a, u64, Vec<ProcessorRecord>>>),
    Default(Once<&'a ProcessorRecord>),
    Empty,
}

impl<'a> Iterator for MatchingRecords<'a> {
    type Item = &'a ProcessorRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MatchingRecords::Values(values) => values.next(),
            MatchingRecords::Default(default) => default.next(),
            MatchingRecords::Empty => None,
        }
    }
}

fn get_record_key_hash(record: &Record, key_indexes: &[usize]) -> u64 {
    let key_fields = key_indexes.iter().map(|i| &record.values[*i]);
    get_record_hash(key_fields)
}

fn get_record_key_fields(record: &Record, key_indexes: &[usize]) -> Vec<Field> {
    key_indexes
        .iter()
        .map(|i| record.values[*i].clone())
        .collect()
}

fn remove_record_using_primary_key(
    mut record_map: hash_map::OccupiedEntry<JoinKey, HashMap<u64, Vec<ProcessorRecord>>>,
    primary_key: u64,
) {
    if let hash_map::Entry::Occupied(mut record_vec) = record_map.get_mut().entry(primary_key) {
        record_vec.get_mut().pop();
        if record_vec.get().is_empty() {
            record_vec.remove();
        }
    }

    if record_map.get().is_empty() {
        record_map.remove();
    }
}

fn serialize_join_map(
    join_map: &HashMap<RecordKey, HashMap<u64, Vec<ProcessorRecord>>>,
    record_store: &ProcessorRecordStore,
    object: &mut Object,
) -> Result<(), SerializationError> {
    serialize_u64(join_map.len() as u64, object)?;
    for (key, value) in join_map {
        serialize_bincode(key, object)?;
        serialize_map(value, record_store, object)?;
    }
    Ok(())
}

fn deserialize_join_map(
    cursor: &mut Cursor,
    record_store: &ProcessorRecordStoreDeserializer,
) -> Result<HashMap<RecordKey, HashMap<u64, Vec<ProcessorRecord>>>, DeserializationError> {
    let len = deserialize_u64(cursor)? as usize;
    let mut map = HashMap::with_capacity(len);
    for _ in 0..len {
        let key = deserialize_bincode(cursor)?;
        let value = deserialize_map(cursor, record_store)?;
        map.insert(key, value);
    }
    Ok(map)
}

fn serialize_map(
    map: &HashMap<u64, Vec<ProcessorRecord>>,
    record_store: &ProcessorRecordStore,
    object: &mut Object,
) -> Result<(), SerializationError> {
    serialize_u64(map.len() as u64, object)?;
    for (key, value) in map {
        serialize_u64(*key, object)?;
        serialize_vec(value, record_store, object)?;
    }
    Ok(())
}

fn deserialize_map(
    cursor: &mut Cursor,
    record_store: &ProcessorRecordStoreDeserializer,
) -> Result<HashMap<u64, Vec<ProcessorRecord>>, DeserializationError> {
    let len = deserialize_u64(cursor)? as usize;
    let mut map = HashMap::with_capacity(len);
    for _ in 0..len {
        let key = deserialize_u64(cursor)?;
        let value = deserialize_vec(cursor, record_store)?;
        map.insert(key, value);
    }
    Ok(map)
}

fn serialize_vec(
    vec: &[ProcessorRecord],
    record_store: &ProcessorRecordStore,
    object: &mut Object,
) -> Result<(), SerializationError> {
    serialize_u64(vec.len() as u64, object)?;
    for record in vec {
        serialize_record(record, record_store, object)?;
    }
    Ok(())
}

fn deserialize_vec(
    cursor: &mut Cursor,
    record_store: &ProcessorRecordStoreDeserializer,
) -> Result<Vec<ProcessorRecord>, DeserializationError> {
    let len = deserialize_u64(cursor)? as usize;
    let mut vec = Vec::with_capacity(len);
    for _ in 0..len {
        vec.push(deserialize_record(cursor, record_store)?);
    }
    Ok(vec)
}

#[cfg(test)]
mod tests {
    use dozer_types::types::{FieldDefinition, FieldType};

    use super::*;

    #[test]
    fn test_match_insert_remove() {
        let schema = Schema {
            fields: vec![FieldDefinition {
                name: "a".to_string(),
                typ: FieldType::Int,
                nullable: false,
                source: Default::default(),
            }],
            primary_index: vec![0],
        };
        let record_store = ProcessorRecordStoreDeserializer::new().unwrap();
        let mut table = JoinTable::new(&schema, vec![0], &record_store, true, None).unwrap();

        let record = Record::new(vec![Field::Int(1)]);
        let join_key = table.get_join_key(&record);
        assert_eq!(table.get_matching_records(&join_key, true).count(), 1);
        assert_eq!(table.get_matching_records(&join_key, false).count(), 0);

        let join_key = table
            .insert(record_store.create_record(&record).unwrap(), &record)
            .unwrap();
        assert_eq!(table.get_matching_records(&join_key, true).count(), 1);
        assert_eq!(table.get_matching_records(&join_key, false).count(), 1);

        let join_key = table.remove(&record);
        assert_eq!(table.get_matching_records(&join_key, true).count(), 1);
        assert_eq!(table.get_matching_records(&join_key, false).count(), 0);
    }
}
