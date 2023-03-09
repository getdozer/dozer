use ahash::AHasher;
use dozer_types::types::{Record, Schema};
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

pub trait JoinOperator: Send + Sync {
    fn delete(&mut self, from: JoinBranch, old: &Record) -> JoinResult<Vec<Record>>;
    fn insert(&mut self, from: JoinBranch, new: &Record) -> JoinResult<Vec<Record>>;
    fn update(&mut self, from: JoinBranch, old: &Record, new: &Record) -> JoinResult<Vec<Record>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    // Left,
    // Right,
}

pub enum JoinAction {
    Insert,
    Delete,
}

#[derive(Debug, Clone)]
pub struct JoinOperation {
    join_type: JoinType,

    left_join_key_indexes: Vec<usize>,
    right_join_key_indexes: Vec<usize>,

    left_map: MultiMap<Vec<u8>, Record>,
    right_map: MultiMap<Vec<u8>, Record>,
}

impl JoinOperation {
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
        join_key: &[u8],
        left_record: &Record,
    ) -> JoinResult<Vec<Record>> {
        let empty_vec = vec![];
        let right_records = self.right_map.get_vec(join_key).unwrap_or(&empty_vec);

        let output_records = right_records
            .iter()
            .map(|right_record| join_records(left_record, right_record))
            .collect::<Vec<Record>>();

        Ok(output_records)
    }

    fn inner_join_from_right(
        &self,
        join_key: &[u8],
        right_record: &Record,
    ) -> JoinResult<Vec<Record>> {
        let empty_vec = vec![];
        let left_records = self.left_map.get_vec(join_key).unwrap_or(&empty_vec);

        let output_records = left_records
            .iter()
            .map(|left_record| join_records(left_record, right_record))
            .collect::<Vec<Record>>();

        Ok(output_records)
    }
}

impl JoinOperator for JoinOperation {
    fn delete(&mut self, from: JoinBranch, old: &Record) -> JoinResult<Vec<Record>> {
        match (&self.join_type, from) {
            (JoinType::Inner, JoinBranch::Left) => {
                let join_key: Vec<u8> = get_join_key(old, &self.left_join_key_indexes);
                let records = self.inner_join_from_left(&join_key, old)?;
                if let Some(map_records) = self.left_map.get_vec_mut(&join_key) {
                    map_records.retain(|x| x != old);
                }
                Ok(records)
            }
            (JoinType::Inner, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_join_key(old, &self.right_join_key_indexes);
                let records = self.inner_join_from_right(&join_key, old)?;
                if let Some(map_records) = self.right_map.get_vec_mut(&join_key) {
                    map_records.retain(|x| x != old);
                }
                Ok(records)
            }
        }
    }

    fn insert(&mut self, from: JoinBranch, new: &Record) -> JoinResult<Vec<Record>> {
        match (&self.join_type, from) {
            (JoinType::Inner, JoinBranch::Left) => {
                let join_key: Vec<u8> = get_join_key(new, &self.left_join_key_indexes);
                let records = self.inner_join_from_left(&join_key, new)?;
                self.left_map.insert(join_key, new.to_owned());

                Ok(records)
            }
            (JoinType::Inner, JoinBranch::Right) => {
                let join_key: Vec<u8> = get_join_key(new, &self.right_join_key_indexes);
                let records = self.inner_join_from_right(&join_key, new)?;
                self.right_map.insert(join_key, new.to_owned());
                Ok(records)
            }
        }
    }

    fn update(&mut self, from: JoinBranch, old: &Record, new: &Record) -> JoinResult<Vec<Record>> {
        todo!()
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

fn append_schema(left_schema: &Schema, right_schema: &Schema) -> Schema {
    let mut output_schema = Schema::empty();

    let left_len = left_schema.fields.len();

    for field in left_schema.fields.iter() {
        output_schema.fields.push(field.clone());
    }

    for field in right_schema.fields.iter() {
        output_schema.fields.push(field.clone());
    }

    for primary_key in left_schema.clone().primary_index.into_iter() {
        output_schema.primary_index.push(primary_key);
    }

    for primary_key in right_schema.clone().primary_index.into_iter() {
        output_schema.primary_index.push(primary_key + left_len);
    }

    output_schema
}
