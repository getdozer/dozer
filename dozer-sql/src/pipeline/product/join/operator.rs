use ahash::AHasher;
use dozer_types::types::{Lifetime, Record};
use linked_hash_map::LinkedHashMap;
use std::{
    collections::HashMap,
    fmt::Debug,
    hash::{Hash, Hasher},
    time::Instant,
};

use super::JoinResult;

pub enum JoinBranch {
    Left,
    Right,
}

// pub trait JoinOperator: Send + Sync {
//     fn delete(&mut self, from: JoinBranch, old: &Record) -> JoinResult<Vec<Record>>;
//     fn insert(&mut self, from: JoinBranch, new: &Record) -> JoinResult<Vec<Record>>;
//     fn update(&mut self, from: JoinBranch, old: &Record, new: &Record) -> JoinResult<Vec<Record>>;
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

type IndexKey = (Vec<u8>, Vec<u8>); // (join_key, primary_key)

#[derive(Debug, Clone)]
pub struct JoinOperator {
    join_type: JoinType,

    left_join_key_indexes: Vec<usize>,
    right_join_key_indexes: Vec<usize>,

    left_primary_key_indexes: Vec<usize>,
    right_primary_key_indexes: Vec<usize>,

    left_default_record: Record,
    right_default_record: Record,

    left_map: HashMap<Vec<u8>, HashMap<Vec<u8>, Vec<Record>>>,
    right_map: HashMap<Vec<u8>, HashMap<Vec<u8>, Vec<Record>>>,

    left_lifetime_map: LinkedHashMap<Instant, Vec<IndexKey>>,
    right_lifetime_map: LinkedHashMap<Instant, Vec<IndexKey>>,
}

impl JoinOperator {
    pub fn new(
        join_type: JoinType,
        left_join_key_indexes: Vec<usize>,
        right_join_key_indexes: Vec<usize>,
        left_primary_key_indexes: Vec<usize>,
        right_primary_key_indexes: Vec<usize>,
        left_default_record: Record,
        right_default_record: Record,
    ) -> Self {
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
        }
    }

    fn inner_join_from_left(
        &self,
        action: &JoinAction,
        join_key: &[u8],
        left_record: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let right_records = get_join_records(&self.right_map, join_key);

        let output_records = right_records
            .iter()
            .map(|right_record| (action.clone(), join_records(left_record, right_record)))
            .collect::<Vec<(JoinAction, Record)>>();

        Ok(output_records)
    }

    fn inner_join_from_right(
        &self,
        action: &JoinAction,
        join_key: &[u8],
        right_record: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let left_records = get_join_records(&self.left_map, join_key);

        let output_records = left_records
            .iter()
            .map(|left_record| (action.clone(), join_records(left_record, right_record)))
            .collect::<Vec<(JoinAction, Record)>>();

        Ok(output_records)
    }

    fn left_join_from_left(
        &self,
        action: &JoinAction,
        join_key: &[u8],
        left_record: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let right_records = get_join_records(&self.right_map, join_key);

        // no joining records on the right branch
        if right_records.is_empty() {
            let join_record = join_records(left_record, &self.right_default_record);
            return Ok(vec![(action.clone(), join_record)]);
        }

        let output_records = right_records
            .iter()
            .map(|right_record| (action.clone(), join_records(left_record, right_record)))
            .collect::<Vec<(JoinAction, Record)>>();

        Ok(output_records)
    }

    fn left_join_from_right(
        &self,
        action: &JoinAction,
        join_key: &[u8],
        right_record: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let left_records = get_join_records(&self.left_map, join_key);

        // if there are no matching records on the left branch, no records will be returned
        if left_records.is_empty() {
            return Ok(vec![]);
        }

        let mut output_records = vec![];

        for left_record in left_records.iter() {
            let right_matching_count = self.get_right_matching_count(action, left_record)?;
            let join_record = join_records(left_record, right_record);

            if right_matching_count > 0 {
                // if there are multiple matching records on the right branch, the left record will be just returned
                output_records.push((action.clone(), join_record));
            } else {
                match action {
                    JoinAction::Insert => {
                        let old_join_record = join_records(left_record, &self.right_default_record);

                        // delete the "first left join" record
                        output_records.push((JoinAction::Delete, old_join_record));
                        // insert the new left join record
                        output_records.push((action.clone(), join_record));
                    }
                    JoinAction::Delete => {
                        let new_join_record = join_records(left_record, &self.right_default_record);

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
        join_key: &[u8],
        left_record: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let right_records = get_join_records(&self.right_map, join_key);

        // if there are no matching records on the left branch, no records will be returned
        if right_records.is_empty() {
            return Ok(vec![]);
        }

        let mut output_records = vec![];

        for right_record in right_records.iter() {
            let left_matching_count = self.get_left_matching_count(action, right_record)?;
            let join_record = join_records(left_record, right_record);

            if left_matching_count > 0 {
                // if there are multiple matching records on the left branch, the right record will be just returned
                output_records.push((action.clone(), join_record));
            } else {
                match action {
                    JoinAction::Insert => {
                        let old_join_record = join_records(&self.left_default_record, right_record);

                        // delete the "first left join" record
                        output_records.push((JoinAction::Delete, old_join_record));
                        // insert the new left join record
                        output_records.push((action.clone(), join_record));
                    }
                    JoinAction::Delete => {
                        let new_join_record = join_records(&self.left_default_record, right_record);

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
        join_key: &[u8],
        right_record: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let left_records = get_join_records(&self.left_map, join_key);

        // no joining records on the right branch
        if left_records.is_empty() {
            let join_record = join_records(&self.left_default_record, right_record);
            return Ok(vec![(action.clone(), join_record)]);
        }

        let output_records = left_records
            .iter()
            .map(|left_record| (action.clone(), join_records(left_record, right_record)))
            .collect::<Vec<(JoinAction, Record)>>();

        Ok(output_records)
    }

    fn get_left_matching_count(&self, action: &JoinAction, record: &Record) -> JoinResult<usize> {
        let join_key: Vec<u8> = get_record_key(record, &self.right_join_key_indexes);

        let mut matching_count = get_join_records(&self.left_map, &join_key).len();
        if action == &JoinAction::Insert {
            matching_count -= 1;
        }
        Ok(matching_count)
    }

    fn get_right_matching_count(&self, action: &JoinAction, record: &Record) -> JoinResult<usize> {
        let join_key: Vec<u8> = get_record_key(record, &self.left_join_key_indexes);

        let mut matching_count = get_join_records(&self.right_map, &join_key).len();
        if action == &JoinAction::Insert {
            matching_count -= 1;
        }
        Ok(matching_count)
    }

    pub fn evict_index(&mut self, from_branch: &JoinBranch, now: &Instant) -> Vec<Instant> {
        let (eviction_index, join_index) = match from_branch {
            JoinBranch::Left => (&self.left_lifetime_map, &mut self.left_map),
            JoinBranch::Right => (&self.right_lifetime_map, &mut self.right_map),
        };

        let mut old_instants = vec![];
        for (eviction_instant, join_index_keys) in eviction_index.iter() {
            if eviction_instant <= now {
                old_instants.push(*eviction_instant);
                for (join_key, primary_key) in join_index_keys.iter() {
                    evict_join_record(join_index, join_key, primary_key);
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
        join_key: &[u8],
        primary_key: &[u8],
    ) {
        let eviction_index = match from_branch {
            JoinBranch::Left => &mut self.left_lifetime_map,
            JoinBranch::Right => &mut self.right_lifetime_map,
        };

        let eviction_instant = Instant::now() + lifetime.duration.0;

        if let Some(join_index_keys) = eviction_index.get_mut(&eviction_instant) {
            join_index_keys.push((join_key.to_owned(), primary_key.to_owned()));
        } else {
            eviction_index.insert(
                eviction_instant,
                vec![(join_key.to_owned(), primary_key.to_owned())],
            );
        }
    }

    pub fn clean_evict_index(&mut self, from_branch: &JoinBranch, old_instants: &[Instant]) {
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
        old: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        match (&self.join_type, from) {
            (JoinType::Inner, JoinBranch::Left) => {
                let join_key: Vec<u8> = get_record_key(old, &self.left_join_key_indexes);

                remove_join_record(
                    &mut self.left_map,
                    &self.left_primary_key_indexes,
                    &join_key,
                    old,
                );

                let records = self.inner_join_from_left(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
            (JoinType::Inner, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_record_key(old, &self.right_join_key_indexes);

                remove_join_record(
                    &mut self.right_map,
                    &self.right_primary_key_indexes,
                    &join_key,
                    old,
                );

                let records = self.inner_join_from_right(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Left) => {
                let join_key: Vec<u8> = get_record_key(old, &self.left_join_key_indexes);
                remove_join_record(
                    &mut self.left_map,
                    &self.left_primary_key_indexes,
                    &join_key,
                    old,
                );
                let records = self.left_join_from_left(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_record_key(old, &self.right_join_key_indexes);
                remove_join_record(
                    &mut self.right_map,
                    &self.right_primary_key_indexes,
                    &join_key,
                    old,
                );
                let records = self.left_join_from_right(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Left) => {
                let join_key: Vec<u8> = get_record_key(old, &self.left_join_key_indexes);
                remove_join_record(
                    &mut self.left_map,
                    &self.left_primary_key_indexes,
                    &join_key,
                    old,
                );
                let records = self.right_join_from_left(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_record_key(old, &self.right_join_key_indexes);
                remove_join_record(
                    &mut self.right_map,
                    &self.right_primary_key_indexes,
                    &join_key,
                    old,
                );
                let records = self.right_join_from_right(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
        }
    }

    pub fn insert(
        &mut self,
        from: &JoinBranch,
        new: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        match (&self.join_type, from) {
            (JoinType::Inner, JoinBranch::Left) => {
                let join_key: Vec<u8> = get_record_key(new, &self.left_join_key_indexes);
                let primary_key: Vec<u8> = get_record_key(new, &self.left_primary_key_indexes);

                add_join_record(&mut self.left_map, &join_key, &primary_key, new);

                if let Some(lifetime) = new.lifetime.clone() {
                    self.insert_evict_index(from, lifetime, &join_key, &primary_key)
                }

                let records = self.inner_join_from_left(&JoinAction::Insert, &join_key, new)?;
                Ok(records)
            }
            (JoinType::Inner, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_record_key(new, &self.right_join_key_indexes);
                let primary_key: Vec<u8> = get_record_key(new, &self.right_primary_key_indexes);

                add_join_record(&mut self.right_map, &join_key, &primary_key, new);

                if let Some(lifetime) = new.lifetime.clone() {
                    self.insert_evict_index(from, lifetime, &join_key, &primary_key)
                }

                let records = self.inner_join_from_right(&JoinAction::Insert, &join_key, new)?;

                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Left) => {
                let join_key: Vec<u8> = get_record_key(new, &self.left_join_key_indexes);
                let primary_key: Vec<u8> = get_record_key(new, &self.left_primary_key_indexes);

                add_join_record(&mut self.left_map, &join_key, &primary_key, new);

                if let Some(lifetime) = new.lifetime.clone() {
                    self.insert_evict_index(from, lifetime, &join_key, &primary_key)
                }

                let records = self.left_join_from_left(&JoinAction::Insert, &join_key, new)?;

                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_record_key(new, &self.right_join_key_indexes);
                let primary_key: Vec<u8> = get_record_key(new, &self.right_primary_key_indexes);

                add_join_record(&mut self.right_map, &join_key, &primary_key, new);

                if let Some(lifetime) = new.lifetime.clone() {
                    self.insert_evict_index(from, lifetime, &join_key, &primary_key)
                }

                let records = self.left_join_from_right(&JoinAction::Insert, &join_key, new)?;

                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Left) => {
                let join_key: Vec<u8> = get_record_key(new, &self.left_join_key_indexes);
                let primary_key: Vec<u8> = get_record_key(new, &self.left_primary_key_indexes);

                add_join_record(&mut self.left_map, &join_key, &primary_key, new);

                if let Some(lifetime) = new.lifetime.clone() {
                    self.insert_evict_index(from, lifetime, &join_key, &primary_key)
                }

                let records = self.right_join_from_left(&JoinAction::Insert, &join_key, new)?;

                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_record_key(new, &self.right_join_key_indexes);
                let primary_key: Vec<u8> = get_record_key(new, &self.right_primary_key_indexes);

                add_join_record(&mut self.right_map, &join_key, &primary_key, new);

                if let Some(lifetime) = new.lifetime.clone() {
                    self.insert_evict_index(from, lifetime, &join_key, &primary_key)
                }

                let records = self.right_join_from_right(&JoinAction::Insert, &join_key, new)?;

                Ok(records)
            }
        }
    }
}

fn add_join_record(
    join_map: &mut HashMap<Vec<u8>, HashMap<Vec<u8>, Vec<Record>>>,
    join_key: &[u8],
    record_key: &[u8],
    record: &Record,
) {
    if let Some(record_map) = join_map.get_mut(join_key) {
        if let Some(record_vec) = record_map.get_mut(record_key) {
            record_vec.push(record.to_owned());
        } else {
            record_map.insert(record_key.to_vec(), vec![record.to_owned()]);
        }
    } else {
        let mut record_map = HashMap::new();
        record_map.insert(record_key.to_vec(), vec![record.to_owned()]);
        join_map.insert(join_key.to_owned(), record_map);
    }
}

fn remove_join_record(
    join_map: &mut HashMap<Vec<u8>, HashMap<Vec<u8>, Vec<Record>>>,
    primary_key_indexes: &[usize],
    join_key: &[u8],
    record: &Record,
) {
    if let Some(record_map) = join_map.get_mut(join_key) {
        let record_key = get_record_key(record, primary_key_indexes);
        if let Some(record_vec) = record_map.get_mut(&record_key) {
            record_vec.pop();
        }
    }
}

fn evict_join_record(
    join_map: &mut HashMap<Vec<u8>, HashMap<Vec<u8>, Vec<Record>>>,
    join_key: &[u8],
    primary_key: &[u8],
) {
    if let Some(record_map) = join_map.get_mut(join_key) {
        if let Some(record_vec) = record_map.get_mut(primary_key) {
            record_vec.pop();
        }
    }
}

fn get_record_key(record: &Record, key_indexes: &[usize]) -> Vec<u8> {
    let mut hasher = AHasher::default();
    for index in key_indexes.iter() {
        record.values[*index].hash(&mut hasher);
    }
    let join_key = hasher.finish();

    join_key.to_be_bytes().to_vec()
}

fn get_join_records(
    join_map: &HashMap<Vec<u8>, HashMap<Vec<u8>, Vec<Record>>>,
    join_key: &[u8],
) -> Vec<Record> {
    let join_map = join_map.get(join_key);

    if let Some(records_map) = join_map {
        records_map.values().flatten().cloned().collect()
    } else {
        vec![]
    }
}

fn join_records(left_record: &Record, right_record: &Record) -> Record {
    let concat_values = [
        left_record.values.as_slice(),
        right_record.values.as_slice(),
    ]
    .concat();
    let mut output_record = Record::new(None, concat_values);

    if let Some(left_record_lifetime) = left_record.lifetime.clone() {
        if let Some(right_record_lifetime) = right_record.lifetime.clone() {
            if left_record_lifetime.reference > right_record_lifetime.reference {
                output_record.set_lifetime(Some(left_record_lifetime));
            } else {
                output_record.set_lifetime(Some(right_record_lifetime));
            }
        } else {
            output_record.set_lifetime(Some(left_record_lifetime));
        }
    } else if let Some(right_record_lifetime) = right_record.lifetime.clone() {
        output_record.set_lifetime(Some(right_record_lifetime));
    }

    output_record
}
