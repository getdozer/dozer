use std::{
    collections::{hash_map::Values, HashMap},
    iter::{once, Flatten, Once},
};

use dozer_core::processor_record::{ProcessorRecord, ProcessorRecordStore};
use dozer_types::{
    chrono,
    types::{Field, Record, Schema, Timestamp},
};
use linked_hash_map::LinkedHashMap;

use crate::pipeline::{
    errors::JoinError,
    utils::record_hashtable_key::{get_record_hash, RecordKey},
};

use super::JoinBranch;

pub type JoinKey = RecordKey;
pub type IndexKey = (JoinKey, u64); // (join_key, primary_key)

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
        record_store: &ProcessorRecordStore,
        accurate_keys: bool,
    ) -> Result<Self, JoinError> {
        let primary_key_indexes = if schema.primary_index.is_empty() {
            (0..schema.fields.len()).collect()
        } else {
            schema.primary_index.clone()
        };
        let default_record = Record::nulls_from_schema(schema);
        let default_record = record_store.create_record(&default_record)?;
        Ok(Self {
            join_key_indexes,
            primary_key_indexes,
            default_record,
            map: HashMap::new(),
            lifetime_map: LinkedHashMap::new(),
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

    pub fn get_join_records<'a>(
        &'a self,
        join_key: &JoinKey,
        record: &'a ProcessorRecord,
        record_branch: JoinBranch,
        default_if_no_match: bool,
    ) -> impl Iterator<Item = ProcessorRecord> + 'a {
        self.get_matching_records(join_key, default_if_no_match)
            .map(move |matching_record| {
                let record = record.clone();
                let matching_record = matching_record.clone();
                match record_branch {
                    JoinBranch::Left => join_records(record, matching_record),
                    JoinBranch::Right => join_records(matching_record, record),
                }
            })
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
        if let Some(record_map) = self.map.get_mut(&join_key) {
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
                    if let Some(record_map) = self.map.get_mut(join_key) {
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

    fn get_join_key(&self, record: &Record) -> JoinKey {
        if self.accurate_keys {
            JoinKey::Accurate(get_record_key_fields(record, &self.join_key_indexes))
        } else {
            JoinKey::Hash(get_record_key_hash(record, &self.join_key_indexes))
        }
    }
}

pub fn join_records(
    left_record: ProcessorRecord,
    right_record: ProcessorRecord,
) -> ProcessorRecord {
    let left_lifetime = left_record.get_lifetime();
    let right_lifetime = right_record.get_lifetime();

    let mut output_record = ProcessorRecord::new();
    output_record.extend(left_record);
    output_record.extend(right_record);

    if let Some(left_record_lifetime) = left_lifetime {
        if let Some(right_record_lifetime) = right_lifetime {
            if left_record_lifetime.reference > right_record_lifetime.reference {
                output_record.set_lifetime(Some(left_record_lifetime));
            } else {
                output_record.set_lifetime(Some(right_record_lifetime));
            }
        } else {
            output_record.set_lifetime(Some(left_record_lifetime));
        }
    } else if let Some(right_record_lifetime) = right_lifetime {
        output_record.set_lifetime(Some(right_record_lifetime));
    }

    output_record
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
    record_map: &mut HashMap<u64, Vec<ProcessorRecord>>,
    primary_key: u64,
) {
    if let Some(record_vec) = record_map.get_mut(&primary_key) {
        record_vec.pop();
    }
}
