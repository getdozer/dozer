use std::{
    collections::{
        hash_map::{self, Values},
        HashMap,
    },
    iter::{once, Flatten, Once},
};

use dozer_types::{
    chrono,
    types::{Field, Record, Schema, Timestamp},
};
use linked_hash_map::LinkedHashMap;

use crate::{
    errors::JoinError,
    utils::record_hashtable_key::{get_record_hash, RecordKey},
};

pub type JoinKey = RecordKey;
type IndexKey = (JoinKey, u64); // (join_key, primary_key)

#[derive(Debug, Clone)]
pub struct JoinTable {
    join_key_indexes: Vec<usize>,
    primary_key_indexes: Vec<usize>,
    default_record: Record,
    map: HashMap<JoinKey, HashMap<u64, Vec<Record>>>,
    lifetime_map: LinkedHashMap<Timestamp, Vec<IndexKey>>,
    accurate_keys: bool,
}

impl JoinTable {
    pub fn new(
        schema: &Schema,
        join_key_indexes: Vec<usize>,
        accurate_keys: bool,
    ) -> Result<Self, JoinError> {
        let primary_key_indexes = if schema.primary_index.is_empty() {
            (0..schema.fields.len()).collect()
        } else {
            schema.primary_index.clone()
        };

        Ok(Self {
            join_key_indexes,
            primary_key_indexes,
            default_record: Record::nulls_from_schema(schema),
            map: Default::default(),
            lifetime_map: Default::default(),
            accurate_keys,
        })
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

    pub fn default_record(&self) -> &Record {
        &self.default_record
    }

    pub fn insert(
        &mut self,
        record: Record,
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
    Values(Flatten<Values<'a, u64, Vec<Record>>>),
    Default(Once<&'a Record>),
    Empty,
}

impl<'a> Iterator for MatchingRecords<'a> {
    type Item = &'a Record;

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
    mut record_map: hash_map::OccupiedEntry<JoinKey, HashMap<u64, Vec<Record>>>,
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
                description: None,
            }],
            primary_index: vec![0],
        };
        let mut table = JoinTable::new(&schema, vec![0], true).unwrap();

        let record = Record::new(vec![Field::Int(1)]);
        let join_key = table.get_join_key(&record);
        assert_eq!(table.get_matching_records(&join_key, true).count(), 1);
        assert_eq!(table.get_matching_records(&join_key, false).count(), 0);

        let join_key = table.insert(record.clone(), &record).unwrap();
        assert_eq!(table.get_matching_records(&join_key, true).count(), 1);
        assert_eq!(table.get_matching_records(&join_key, false).count(), 1);

        let join_key = table.remove(&record);
        assert_eq!(table.get_matching_records(&join_key, true).count(), 1);
        assert_eq!(table.get_matching_records(&join_key, false).count(), 0);
    }
}
