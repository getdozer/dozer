use ahash::AHasher;
use dozer_types::types::Record;
use multimap::MultiMap;
use std::{
    fmt::Debug,
    hash::{Hash, Hasher},
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

#[derive(Debug, Clone)]
pub struct JoinOperator {
    join_type: JoinType,

    left_join_key_indexes: Vec<usize>,
    right_join_key_indexes: Vec<usize>,

    left_default_record: Record,
    right_default_record: Record,

    left_map: MultiMap<Vec<u8>, Record>,
    right_map: MultiMap<Vec<u8>, Record>,
}

impl JoinOperator {
    pub fn new(
        join_type: JoinType,
        left_join_key_indexes: Vec<usize>,
        right_join_key_indexes: Vec<usize>,
        left_default_record: Record,
        right_default_record: Record,
    ) -> Self {
        Self {
            join_type,
            left_join_key_indexes,
            right_join_key_indexes,
            left_default_record,
            right_default_record,
            left_map: MultiMap::new(),
            right_map: MultiMap::new(),
        }
    }

    fn inner_join_from_left(
        &self,
        action: &JoinAction,
        join_key: &[u8],
        left_record: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let empty_vec = vec![];
        let right_records = self.right_map.get_vec(join_key).unwrap_or(&empty_vec);

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
        let empty_vec = vec![];
        let left_records = self.left_map.get_vec(join_key).unwrap_or(&empty_vec);

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
        let empty_vec = vec![];
        let right_records = self.right_map.get_vec(join_key).unwrap_or(&empty_vec);

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
        let empty_vec = vec![];
        let left_records = self.left_map.get_vec(join_key).unwrap_or(&empty_vec);

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
        let empty_vec = vec![];
        let right_records = self.right_map.get_vec(join_key).unwrap_or(&empty_vec);

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
        let empty_vec = vec![];
        let left_records = self.left_map.get_vec(join_key).unwrap_or(&empty_vec);

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
        let join_key: Vec<u8> = get_join_key(record, &self.right_join_key_indexes);

        let mut matching_count = self.left_map.get_vec(&join_key).unwrap_or(&vec![]).len();

        if action == &JoinAction::Insert {
            matching_count -= 1;
        }
        Ok(matching_count)
    }

    fn get_right_matching_count(&self, action: &JoinAction, record: &Record) -> JoinResult<usize> {
        let join_key: Vec<u8> = get_join_key(record, &self.left_join_key_indexes);

        let mut matching_count = self.right_map.get_vec(&join_key).unwrap_or(&vec![]).len();

        if action == &JoinAction::Insert {
            matching_count -= 1;
        }
        Ok(matching_count)
    }

    pub fn delete(
        &mut self,
        from: &JoinBranch,
        old: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        match (&self.join_type, from) {
            (JoinType::Inner, JoinBranch::Left) => {
                let join_key: Vec<u8> = get_join_key(old, &self.left_join_key_indexes);
                if let Some(map_records) = self.left_map.get_vec_mut(&join_key) {
                    if let Some(index) = map_records.iter().position(|x| x == old) {
                        map_records.remove(index);
                    }
                }
                let records = self.inner_join_from_left(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
            (JoinType::Inner, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_join_key(old, &self.right_join_key_indexes);
                if let Some(map_records) = self.right_map.get_vec_mut(&join_key) {
                    if let Some(index) = map_records.iter().position(|x| x == old) {
                        map_records.remove(index);
                    }
                }
                let records = self.inner_join_from_right(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Left) => {
                let join_key: Vec<u8> = get_join_key(old, &self.left_join_key_indexes);
                if let Some(map_records) = self.left_map.get_vec_mut(&join_key) {
                    if let Some(index) = map_records.iter().position(|x| x == old) {
                        map_records.remove(index);
                    }
                }
                let records = self.left_join_from_left(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_join_key(old, &self.right_join_key_indexes);
                if let Some(map_records) = self.right_map.get_vec_mut(&join_key) {
                    if let Some(index) = map_records.iter().position(|x| x == old) {
                        map_records.remove(index);
                    }
                }
                let records = self.left_join_from_right(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Left) => {
                let join_key: Vec<u8> = get_join_key(old, &self.left_join_key_indexes);
                if let Some(map_records) = self.left_map.get_vec_mut(&join_key) {
                    if let Some(index) = map_records.iter().position(|x| x == old) {
                        map_records.remove(index);
                    }
                }
                let records = self.right_join_from_left(&JoinAction::Delete, &join_key, old)?;
                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_join_key(old, &self.right_join_key_indexes);
                if let Some(map_records) = self.right_map.get_vec_mut(&join_key) {
                    if let Some(index) = map_records.iter().position(|x| x == old) {
                        map_records.remove(index);
                    }
                }
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
                let join_key: Vec<u8> = get_join_key(new, &self.left_join_key_indexes);
                self.left_map.insert(join_key.clone(), new.to_owned());
                let records = self.inner_join_from_left(&JoinAction::Insert, &join_key, new)?;
                Ok(records)
            }
            (JoinType::Inner, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_join_key(new, &self.right_join_key_indexes);
                self.right_map.insert(join_key.clone(), new.to_owned());
                let records = self.inner_join_from_right(&JoinAction::Insert, &join_key, new)?;

                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Left) => {
                let join_key: Vec<u8> = get_join_key(new, &self.left_join_key_indexes);
                self.left_map.insert(join_key.clone(), new.to_owned());
                let records = self.left_join_from_left(&JoinAction::Insert, &join_key, new)?;

                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_join_key(new, &self.right_join_key_indexes);
                self.right_map.insert(join_key.clone(), new.to_owned());
                let records = self.left_join_from_right(&JoinAction::Insert, &join_key, new)?;

                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Left) => {
                let join_key: Vec<u8> = get_join_key(new, &self.left_join_key_indexes);
                self.left_map.insert(join_key.clone(), new.to_owned());
                let records = self.right_join_from_left(&JoinAction::Insert, &join_key, new)?;

                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_join_key(new, &self.right_join_key_indexes);
                self.right_map.insert(join_key.clone(), new.to_owned());
                let records = self.right_join_from_right(&JoinAction::Insert, &join_key, new)?;

                Ok(records)
            }
        }
    }
}

fn get_join_key(old: &Record, join_key_indexes: &[usize]) -> Vec<u8> {
    let mut hasher = AHasher::default();
    for index in join_key_indexes.iter() {
        old.values[*index].hash(&mut hasher);
    }
    let join_key = hasher.finish();

    join_key.to_be_bytes().to_vec()
}

fn join_records(left_record: &Record, right_record: &Record) -> Record {
    let concat_values = [
        left_record.values.as_slice(),
        right_record.values.as_slice(),
    ]
    .concat();
    Record::new(None, concat_values, None)
}
