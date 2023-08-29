use crate::pipeline::utils::record_hashtable_key::{get_record_hash, RecordKey};
use dozer_core::processor_record::{ProcessorRecord, ProcessorRecordStore};
use dozer_types::{
    chrono,
    types::{Field, Lifetime, Record, Timestamp},
};
use linked_hash_map::LinkedHashMap;
use std::{collections::HashMap, fmt::Debug};

use crate::pipeline::errors::JoinError;

use super::JoinResult;

pub enum JoinBranch {
    Left,
    Right,
}

// pub trait JoinOperator: Send + Sync {
//     fn delete(&mut self, from: JoinBranch, old: &ProcessorRecord) -> JoinResult<Vec<Record>>;
//     fn insert(&mut self, from: JoinBranch, new: &ProcessorRecord) -> JoinResult<Vec<Record>>;
//     fn update(&mut self, from: JoinBranch, old: &ProcessorRecord, new: &ProcessorRecord) -> JoinResult<Vec<Record>>;
// }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinAction {
    Insert,
    Delete,
}

type JoinKey = RecordKey;
type IndexKey = (JoinKey, u64); // (join_key, primary_key)

#[derive(Debug, Clone)]
pub struct JoinOperator {
    join_type: JoinType,

    left_join_key_indexes: Vec<usize>,
    right_join_key_indexes: Vec<usize>,

    left_primary_key_indexes: Vec<usize>,
    right_primary_key_indexes: Vec<usize>,

    left_default_record: ProcessorRecord,
    right_default_record: ProcessorRecord,

    left_map: HashMap<JoinKey, HashMap<u64, Vec<ProcessorRecord>>>,
    right_map: HashMap<JoinKey, HashMap<u64, Vec<ProcessorRecord>>>,

    left_lifetime_map: LinkedHashMap<Timestamp, Vec<IndexKey>>,
    right_lifetime_map: LinkedHashMap<Timestamp, Vec<IndexKey>>,

    accurate_keys: bool,
}

impl JoinOperator {
    pub fn new(
        join_type: JoinType,
        (left_join_key_indexes, right_join_key_indexes): (Vec<usize>, Vec<usize>),
        (left_primary_key_indexes, right_primary_key_indexes): (Vec<usize>, Vec<usize>),
        (left_default_record, right_default_record): (ProcessorRecord, ProcessorRecord),
        enable_probabilistic_optimizations: bool,
    ) -> Self {
        let accurate_keys = !enable_probabilistic_optimizations;
        Self {
            join_type,
            left_join_key_indexes,
            right_join_key_indexes,
            left_primary_key_indexes,
            right_primary_key_indexes,
            left_default_record,
            right_default_record,
            left_map: HashMap::new(),
            right_map: HashMap::new(),
            left_lifetime_map: LinkedHashMap::new(),
            right_lifetime_map: LinkedHashMap::new(),
            accurate_keys,
        }
    }

    pub fn left_lookup_size(&self) -> usize {
        self.left_map.len()
    }

    pub fn right_lookup_size(&self) -> usize {
        self.right_map.len()
    }

    fn inner_join_from_left(
        &self,
        action: &JoinAction,
        join_key: &JoinKey,
        left_record: ProcessorRecord,
    ) -> JoinResult<Vec<(JoinAction, ProcessorRecord)>> {
        let right_records = get_join_records(&self.right_map, join_key);

        let output_records = right_records
            .into_iter()
            .map(|right_record| {
                (
                    action.clone(),
                    join_records(left_record.clone(), right_record),
                )
            })
            .collect::<Vec<(JoinAction, ProcessorRecord)>>();

        Ok(output_records)
    }

    fn inner_join_from_right(
        &self,
        action: &JoinAction,
        join_key: &JoinKey,
        right_record: ProcessorRecord,
    ) -> JoinResult<Vec<(JoinAction, ProcessorRecord)>> {
        let left_records = get_join_records(&self.left_map, join_key);

        let output_records = left_records
            .into_iter()
            .map(|left_record| {
                (
                    action.clone(),
                    join_records(left_record, right_record.clone()),
                )
            })
            .collect::<Vec<(JoinAction, ProcessorRecord)>>();

        Ok(output_records)
    }

    fn left_join_from_left(
        &self,
        action: &JoinAction,
        join_key: &JoinKey,
        left_record: ProcessorRecord,
    ) -> JoinResult<Vec<(JoinAction, ProcessorRecord)>> {
        let right_records = get_join_records(&self.right_map, join_key);

        // no joining records on the right branch
        if right_records.is_empty() {
            let join_record = join_records(left_record, self.right_default_record.clone());
            return Ok(vec![(action.clone(), join_record)]);
        }

        let output_records = right_records
            .into_iter()
            .map(|right_record| {
                (
                    action.clone(),
                    join_records(left_record.clone(), right_record),
                )
            })
            .collect::<Vec<(JoinAction, ProcessorRecord)>>();

        Ok(output_records)
    }

    fn left_join_from_right(
        &self,
        action: &JoinAction,
        join_key: &JoinKey,
        record_store: &ProcessorRecordStore,
        right_record: ProcessorRecord,
    ) -> JoinResult<Vec<(JoinAction, ProcessorRecord)>> {
        let left_records = get_join_records(&self.left_map, join_key);

        // if there are no matching records on the left branch, no records will be returned
        if left_records.is_empty() {
            return Ok(vec![]);
        }

        let mut output_records = vec![];

        for left_record in left_records.into_iter() {
            let left_record_decoded = record_store.load_record(&left_record)?;
            let right_matching_count =
                self.get_right_matching_count(action, &left_record_decoded)?;
            let join_record = join_records(left_record.clone(), right_record.clone());

            if right_matching_count > 0 {
                // if there are multiple matching records on the right branch, the left record will be just returned
                output_records.push((action.clone(), join_record));
            } else {
                match action {
                    JoinAction::Insert => {
                        let old_join_record =
                            join_records(left_record, self.right_default_record.clone());

                        // delete the "first left join" record
                        output_records.push((JoinAction::Delete, old_join_record));
                        // insert the new left join record
                        output_records.push((action.clone(), join_record));
                    }
                    JoinAction::Delete => {
                        let new_join_record =
                            join_records(left_record, self.right_default_record.clone());

                        output_records.push((JoinAction::Delete, join_record));
                        output_records.push((JoinAction::Insert, new_join_record));
                    }
                }
            }
        }
        Ok(output_records)
    }

    fn right_join_from_left(
        &self,
        action: &JoinAction,
        join_key: &JoinKey,
        record_store: &ProcessorRecordStore,
        left_record: ProcessorRecord,
    ) -> JoinResult<Vec<(JoinAction, ProcessorRecord)>> {
        let right_records = get_join_records(&self.right_map, join_key);

        // if there are no matching records on the left branch, no records will be returned
        if right_records.is_empty() {
            return Ok(vec![]);
        }

        let mut output_records = vec![];

        for right_record in right_records.into_iter() {
            let right_record_decoded = record_store.load_record(&right_record)?;
            let left_matching_count =
                self.get_left_matching_count(action, &right_record_decoded)?;
            let join_record = join_records(left_record.clone(), right_record.clone());

            if left_matching_count > 0 {
                // if there are multiple matching records on the left branch, the right record will be just returned
                output_records.push((action.clone(), join_record));
            } else {
                match action {
                    JoinAction::Insert => {
                        let old_join_record =
                            join_records(self.left_default_record.clone(), right_record);

                        // delete the "first left join" record
                        output_records.push((JoinAction::Delete, old_join_record));
                        // insert the new left join record
                        output_records.push((action.clone(), join_record));
                    }
                    JoinAction::Delete => {
                        let new_join_record =
                            join_records(self.left_default_record.clone(), right_record);

                        output_records.push((JoinAction::Delete, join_record));
                        output_records.push((JoinAction::Insert, new_join_record));
                    }
                }
            }
        }
        Ok(output_records)
    }

    fn right_join_from_right(
        &self,
        action: &JoinAction,
        join_key: &JoinKey,
        right_record: ProcessorRecord,
    ) -> JoinResult<Vec<(JoinAction, ProcessorRecord)>> {
        let left_records = get_join_records(&self.left_map, join_key);

        // no joining records on the right branch
        if left_records.is_empty() {
            let join_record = join_records(self.left_default_record.clone(), right_record);
            return Ok(vec![(action.clone(), join_record)]);
        }

        let output_records = left_records
            .into_iter()
            .map(|left_record| {
                (
                    action.clone(),
                    join_records(left_record, right_record.clone()),
                )
            })
            .collect::<Vec<(JoinAction, ProcessorRecord)>>();

        Ok(output_records)
    }

    fn get_left_matching_count(&self, action: &JoinAction, record: &Record) -> JoinResult<usize> {
        let join_key = self.get_join_key(record, &self.right_join_key_indexes);

        let mut matching_count = get_join_records(&self.left_map, &join_key).len();
        if action == &JoinAction::Insert {
            matching_count -= 1;
        }
        Ok(matching_count)
    }

    fn get_right_matching_count(&self, action: &JoinAction, record: &Record) -> JoinResult<usize> {
        let join_key = self.get_join_key(record, &self.left_join_key_indexes);

        let mut matching_count = get_join_records(&self.right_map, &join_key).len();
        if action == &JoinAction::Insert {
            matching_count -= 1;
        }
        Ok(matching_count)
    }

    pub fn evict_index(&mut self, from_branch: &JoinBranch, now: &Timestamp) -> Vec<Timestamp> {
        let (eviction_index, join_index) = match from_branch {
            JoinBranch::Left => (&self.left_lifetime_map, &mut self.left_map),
            JoinBranch::Right => (&self.right_lifetime_map, &mut self.right_map),
        };

        let mut old_instants = vec![];
        for (eviction_instant, join_index_keys) in eviction_index.iter() {
            if eviction_instant <= now {
                old_instants.push(*eviction_instant);
                for (join_key, primary_key) in join_index_keys {
                    evict_join_record(join_index, join_key, *primary_key);
                }
            } else {
                break;
            }
        }
        old_instants
    }

    pub fn insert_evict_index(
        &mut self,
        from_branch: &JoinBranch,
        lifetime: Lifetime,
        join_key: JoinKey,
        primary_key: u64,
    ) -> JoinResult<()> {
        let eviction_index = match from_branch {
            JoinBranch::Left => &mut self.left_lifetime_map,
            JoinBranch::Right => &mut self.right_lifetime_map,
        };

        let eviction_time = {
            let eviction_time_result =
                lifetime
                    .reference
                    .checked_add_signed(chrono::Duration::nanoseconds(
                        lifetime.duration.as_nanos() as i64,
                    ));

            if let Some(eviction_time) = eviction_time_result {
                eviction_time
            } else {
                return Err(JoinError::EvictionTimeOverflow);
            }
        };

        if let Some(join_index_keys) = eviction_index.get_mut(&eviction_time) {
            join_index_keys.push((join_key, primary_key));
        } else {
            eviction_index.insert(eviction_time, vec![(join_key, primary_key)]);
        }

        Ok(())
    }

    pub fn clean_evict_index(&mut self, from_branch: &JoinBranch, old_instants: &[Timestamp]) {
        let eviction_index = match from_branch {
            JoinBranch::Left => &mut self.left_lifetime_map,
            JoinBranch::Right => &mut self.right_lifetime_map,
        };
        for old_instant in old_instants {
            eviction_index.remove(old_instant);
        }
    }

    pub fn delete(
        &mut self,
        from: &JoinBranch,
        record_store: &ProcessorRecordStore,
        old: ProcessorRecord,
        old_decoded: Record,
    ) -> JoinResult<Vec<(JoinAction, ProcessorRecord)>> {
        match (&self.join_type, from) {
            (JoinType::Inner, JoinBranch::Left) => {
                let join_key = self.get_join_key(&old_decoded, &self.left_join_key_indexes);

                remove_join_record(
                    &mut self.left_map,
                    &self.left_primary_key_indexes,
                    &join_key,
                    &old_decoded,
                );

                let records = self.inner_join_from_left(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
            (JoinType::Inner, JoinBranch::Right) => {
                let join_key = self.get_join_key(&old_decoded, &self.right_join_key_indexes);

                remove_join_record(
                    &mut self.right_map,
                    &self.right_primary_key_indexes,
                    &join_key,
                    &old_decoded,
                );

                let records = self.inner_join_from_right(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Left) => {
                let join_key = self.get_join_key(&old_decoded, &self.left_join_key_indexes);
                remove_join_record(
                    &mut self.left_map,
                    &self.left_primary_key_indexes,
                    &join_key,
                    &old_decoded,
                );
                let records = self.left_join_from_left(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Right) => {
                let join_key = self.get_join_key(&old_decoded, &self.right_join_key_indexes);
                remove_join_record(
                    &mut self.right_map,
                    &self.right_primary_key_indexes,
                    &join_key,
                    &old_decoded,
                );
                let records =
                    self.left_join_from_right(&JoinAction::Delete, &join_key, record_store, old)?;
                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Left) => {
                let join_key = self.get_join_key(&old_decoded, &self.left_join_key_indexes);
                remove_join_record(
                    &mut self.left_map,
                    &self.left_primary_key_indexes,
                    &join_key,
                    &old_decoded,
                );
                let records =
                    self.right_join_from_left(&JoinAction::Delete, &join_key, record_store, old)?;
                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Right) => {
                let join_key = self.get_join_key(&old_decoded, &self.right_join_key_indexes);
                remove_join_record(
                    &mut self.right_map,
                    &self.right_primary_key_indexes,
                    &join_key,
                    &old_decoded,
                );
                let records = self.right_join_from_right(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
        }
    }

    pub fn insert(
        &mut self,
        from: &JoinBranch,
        record_store: &ProcessorRecordStore,
        new: ProcessorRecord,
        new_decoded: Record,
    ) -> JoinResult<Vec<(JoinAction, ProcessorRecord)>> {
        match (&self.join_type, from) {
            (JoinType::Inner, JoinBranch::Left) => {
                let join_key = self.get_join_key(&new_decoded, &self.left_join_key_indexes);
                let primary_key = get_record_key_hash(&new_decoded, &self.left_primary_key_indexes);

                add_join_record(&mut self.left_map, join_key.clone(), primary_key, &new);

                if let Some(lifetime) = new.get_lifetime() {
                    self.insert_evict_index(from, lifetime, join_key.clone(), primary_key)?
                }

                let records = self.inner_join_from_left(&JoinAction::Insert, &join_key, new)?;
                Ok(records)
            }
            (JoinType::Inner, JoinBranch::Right) => {
                let join_key = self.get_join_key(&new_decoded, &self.right_join_key_indexes);
                let primary_key =
                    get_record_key_hash(&new_decoded, &self.right_primary_key_indexes);

                add_join_record(&mut self.right_map, join_key.clone(), primary_key, &new);

                if let Some(lifetime) = new.get_lifetime() {
                    self.insert_evict_index(from, lifetime, join_key.clone(), primary_key)?
                }

                let records = self.inner_join_from_right(&JoinAction::Insert, &join_key, new)?;

                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Left) => {
                let join_key = self.get_join_key(&new_decoded, &self.left_join_key_indexes);
                let primary_key = get_record_key_hash(&new_decoded, &self.left_primary_key_indexes);

                add_join_record(&mut self.left_map, join_key.clone(), primary_key, &new);

                if let Some(lifetime) = new.get_lifetime() {
                    self.insert_evict_index(from, lifetime, join_key.clone(), primary_key)?
                }

                let records = self.left_join_from_left(&JoinAction::Insert, &join_key, new)?;

                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Right) => {
                let join_key = self.get_join_key(&new_decoded, &self.right_join_key_indexes);
                let primary_key =
                    get_record_key_hash(&new_decoded, &self.right_primary_key_indexes);

                add_join_record(&mut self.right_map, join_key.clone(), primary_key, &new);

                if let Some(lifetime) = new.get_lifetime() {
                    self.insert_evict_index(from, lifetime, join_key.clone(), primary_key)?
                }

                let records =
                    self.left_join_from_right(&JoinAction::Insert, &join_key, record_store, new)?;

                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Left) => {
                let join_key = self.get_join_key(&new_decoded, &self.left_join_key_indexes);
                let primary_key = get_record_key_hash(&new_decoded, &self.left_primary_key_indexes);

                add_join_record(&mut self.left_map, join_key.clone(), primary_key, &new);

                if let Some(lifetime) = new.get_lifetime() {
                    self.insert_evict_index(from, lifetime, join_key.clone(), primary_key)?
                }

                let records =
                    self.right_join_from_left(&JoinAction::Insert, &join_key, record_store, new)?;

                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Right) => {
                let join_key = self.get_join_key(&new_decoded, &self.right_join_key_indexes);
                let primary_key =
                    get_record_key_hash(&new_decoded, &self.right_primary_key_indexes);

                add_join_record(&mut self.right_map, join_key.clone(), primary_key, &new);

                if let Some(lifetime) = new.get_lifetime() {
                    self.insert_evict_index(from, lifetime, join_key.clone(), primary_key)?
                }

                let records = self.right_join_from_right(&JoinAction::Insert, &join_key, new)?;

                Ok(records)
            }
        }
    }

    fn get_join_key(&self, record: &Record, key_indexes: &[usize]) -> JoinKey {
        if self.accurate_keys {
            JoinKey::Accurate(get_record_key_fields(record, key_indexes))
        } else {
            JoinKey::Hash(get_record_key_hash(record, key_indexes))
        }
    }
}

fn add_join_record(
    join_map: &mut HashMap<JoinKey, HashMap<u64, Vec<ProcessorRecord>>>,
    join_key: JoinKey,
    record_key: u64,
    record: &ProcessorRecord,
) {
    if let Some(record_map) = join_map.get_mut(&join_key) {
        if let Some(record_vec) = record_map.get_mut(&record_key) {
            record_vec.push(record.clone());
        } else {
            record_map.insert(record_key, vec![record.clone()]);
        }
    } else {
        let mut record_map = HashMap::new();
        record_map.insert(record_key, vec![record.clone()]);
        join_map.insert(join_key, record_map);
    }
}

fn remove_join_record(
    join_map: &mut HashMap<JoinKey, HashMap<u64, Vec<ProcessorRecord>>>,
    primary_key_indexes: &[usize],
    join_key: &JoinKey,
    record: &Record,
) {
    if let Some(record_map) = join_map.get_mut(join_key) {
        let record_key = get_record_key_hash(record, primary_key_indexes);
        if let Some(record_vec) = record_map.get_mut(&record_key) {
            record_vec.pop();
        }
    }
}

fn evict_join_record(
    join_map: &mut HashMap<JoinKey, HashMap<u64, Vec<ProcessorRecord>>>,
    join_key: &JoinKey,
    primary_key: u64,
) {
    if let Some(record_map) = join_map.get_mut(join_key) {
        if let Some(record_vec) = record_map.get_mut(&primary_key) {
            record_vec.pop();
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

fn get_join_records(
    join_map: &HashMap<JoinKey, HashMap<u64, Vec<ProcessorRecord>>>,
    join_key: &JoinKey,
) -> Vec<ProcessorRecord> {
    let join_map = join_map.get(join_key);

    if let Some(records_map) = join_map {
        records_map.values().flatten().cloned().collect()
    } else {
        vec![]
    }
}

fn join_records(left_record: ProcessorRecord, right_record: ProcessorRecord) -> ProcessorRecord {
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
