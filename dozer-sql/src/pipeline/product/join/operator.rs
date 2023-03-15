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

#[derive(Debug, Clone)]
pub enum JoinAction {
    Insert,
    Delete,
}

#[derive(Debug, Clone)]
pub struct JoinOperator {
    join_type: JoinType,

    left_join_key_indexes: Vec<usize>,
    right_join_key_indexes: Vec<usize>,

    left_map: MultiMap<Vec<u8>, Record>,
    right_map: MultiMap<Vec<u8>, Record>,
}

impl JoinOperator {
    pub fn new(
        join_type: JoinType,
        left_join_key_indexes: Vec<usize>,
        right_join_key_indexes: Vec<usize>,
    ) -> Self {
        Self {
            join_type,
            left_join_key_indexes,
            right_join_key_indexes,
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

    pub fn delete(
        &mut self,
        from: &JoinBranch,
        old: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        match (&self.join_type, from) {
            (JoinType::Inner, JoinBranch::Left) => {
                let join_key: Vec<u8> = get_join_key(old, &self.left_join_key_indexes);
                let records = self.inner_join_from_left(&JoinAction::Delete, &join_key, old)?;
                if let Some(map_records) = self.left_map.get_vec_mut(&join_key) {
                    map_records.retain(|x| x != old);
                }
                Ok(records)
            }
            (JoinType::Inner, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_join_key(old, &self.right_join_key_indexes);
                let records = self.inner_join_from_right(&JoinAction::Delete, &join_key, old)?;
                if let Some(map_records) = self.right_map.get_vec_mut(&join_key) {
                    map_records.retain(|x| x != old);
                }
                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Left) => todo!(),
            (JoinType::LeftOuter, JoinBranch::Right) => todo!(),
            (JoinType::RightOuter, JoinBranch::Left) => todo!(),
            (JoinType::RightOuter, JoinBranch::Right) => todo!(),
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
                let records = self.inner_join_from_left(&JoinAction::Insert, &join_key, new)?;
                self.left_map.insert(join_key, new.to_owned());

                Ok(records)
            }
            (JoinType::Inner, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_join_key(new, &self.right_join_key_indexes);
                let records = self.inner_join_from_right(&JoinAction::Insert, &join_key, new)?;
                self.right_map.insert(join_key, new.to_owned());
                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Left) => todo!(),
            (JoinType::LeftOuter, JoinBranch::Right) => todo!(),
            (JoinType::RightOuter, JoinBranch::Left) => todo!(),
            (JoinType::RightOuter, JoinBranch::Right) => todo!(),
        }
    }
}

fn get_join_key(old: &Record, right_join_key_indexes: &[usize]) -> Vec<u8> {
    let mut hasher = AHasher::default();
    for index in right_join_key_indexes.iter() {
        old.values[*index].hash(&mut hasher);
    }
    let join_key = hasher.finish();

    join_key.to_be_bytes().to_vec()
}

fn join_records(left_record: &Record, right_record: &Record) -> Record {
    let concat_values = [left_record.values.clone(), right_record.values.clone()].concat();
    Record::new(None, concat_values, None)
}
