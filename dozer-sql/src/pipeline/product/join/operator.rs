use ahash::AHasher;
use dozer_storage::{errors::StorageError, RocksdbMap};
use dozer_types::{borrow::IntoOwned, types::Record};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    hash::{Hash, Hasher},
};
use tempdir::TempDir;

use crate::pipeline::errors::JoinError;

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

#[derive(Debug)]
pub struct JoinOperator {
    join_type: JoinType,

    /// We're using temp dir because currently we don't support restarting app anyway.
    _temp_dir: TempDir,

    left_table: TableForJoin,
    right_table: TableForJoin,
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
    ) -> JoinResult<Self> {
        let temp_dir = TempDir::new("join").map_err(JoinError::CreateTempDir)?;
        let temp_dir_path = temp_dir.path().to_str().expect("path must be utf8");

        let left_table = TableForJoin::new(
            left_join_key_indexes,
            left_primary_key_indexes,
            left_default_record,
            &format!("{}/left", temp_dir_path),
        )?;
        let right_table = TableForJoin::new(
            right_join_key_indexes,
            right_primary_key_indexes,
            right_default_record,
            &format!("{}/right", temp_dir_path),
        )?;

        Ok(Self {
            join_type,
            _temp_dir: temp_dir,
            left_table,
            right_table,
        })
    }

    pub fn left_lookup_size(&mut self) -> Result<usize, StorageError> {
        self.left_table.lookup_size()
    }

    pub fn right_lookup_size(&mut self) -> Result<usize, StorageError> {
        self.right_table.lookup_size()
    }

    fn inner_join_from_left(
        &mut self,
        action: &JoinAction,
        join_key: u64,
        left_record: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let right_records = self.right_table.get_records_from_join_key(join_key)?;

        let output_records = right_records
            .into_iter()
            .map(|right_record| {
                (
                    action.clone(),
                    join_records(left_record.clone(), right_record),
                )
            })
            .collect::<Vec<(JoinAction, Record)>>();

        Ok(output_records)
    }

    fn inner_join_from_right(
        &mut self,
        action: &JoinAction,
        join_key: u64,
        right_record: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let left_records = self.left_table.get_records_from_join_key(join_key)?;

        let output_records = left_records
            .into_iter()
            .map(|left_record| {
                (
                    action.clone(),
                    join_records(left_record, right_record.clone()),
                )
            })
            .collect::<Vec<(JoinAction, Record)>>();

        Ok(output_records)
    }

    fn left_join_from_left(
        &mut self,
        action: &JoinAction,
        join_key: u64,
        left_record: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let right_records = self.right_table.get_records_from_join_key(join_key)?;

        // no joining records on the right branch
        if right_records.is_empty() {
            let join_record =
                join_records(left_record.clone(), self.right_table.default_record.clone());
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
            .collect::<Vec<(JoinAction, Record)>>();

        Ok(output_records)
    }

    fn left_join_from_right(
        &mut self,
        action: &JoinAction,
        join_key: u64,
        right_record: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let left_records = self.left_table.get_records_from_join_key(join_key)?;

        // if there are no matching records on the left branch, no records will be returned
        if left_records.is_empty() {
            return Ok(vec![]);
        }

        let mut output_records = vec![];

        for left_record in left_records {
            let right_matching_count = self.get_right_matching_count(action, &left_record)?;
            let join_record = join_records(left_record.clone(), right_record.clone());

            if right_matching_count > 0 {
                // if there are multiple matching records on the right branch, the left record will be just returned
                output_records.push((action.clone(), join_record));
            } else {
                match action {
                    JoinAction::Insert => {
                        let old_join_record =
                            join_records(left_record, self.right_table.default_record.clone());

                        // delete the "first left join" record
                        output_records.push((JoinAction::Delete, old_join_record));
                        // insert the new left join record
                        output_records.push((action.clone(), join_record));
                    }
                    JoinAction::Delete => {
                        let new_join_record =
                            join_records(left_record, self.right_table.default_record.clone());

                        output_records.push((JoinAction::Delete, join_record));
                        output_records.push((JoinAction::Insert, new_join_record));
                    }
                }
            }
        }
        Ok(output_records)
    }

    fn right_join_from_left(
        &mut self,
        action: &JoinAction,
        join_key: u64,
        left_record: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let right_records = self.right_table.get_records_from_join_key(join_key)?;

        // if there are no matching records on the left branch, no records will be returned
        if right_records.is_empty() {
            return Ok(vec![]);
        }

        let mut output_records = vec![];

        for right_record in right_records {
            let left_matching_count = self.get_left_matching_count(action, &right_record)?;
            let join_record = join_records(left_record.clone(), right_record.clone());

            if left_matching_count > 0 {
                // if there are multiple matching records on the left branch, the right record will be just returned
                output_records.push((action.clone(), join_record));
            } else {
                match action {
                    JoinAction::Insert => {
                        let old_join_record =
                            join_records(self.left_table.default_record.clone(), right_record);

                        // delete the "first left join" record
                        output_records.push((JoinAction::Delete, old_join_record));
                        // insert the new left join record
                        output_records.push((action.clone(), join_record));
                    }
                    JoinAction::Delete => {
                        let new_join_record =
                            join_records(self.left_table.default_record.clone(), right_record);

                        output_records.push((JoinAction::Delete, join_record));
                        output_records.push((JoinAction::Insert, new_join_record));
                    }
                }
            }
        }
        Ok(output_records)
    }

    fn right_join_from_right(
        &mut self,
        action: &JoinAction,
        join_key: u64,
        right_record: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let left_records = self.left_table.get_records_from_join_key(join_key)?;

        // no joining records on the right branch
        if left_records.is_empty() {
            let join_record =
                join_records(self.left_table.default_record.clone(), right_record.clone());
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
            .collect::<Vec<(JoinAction, Record)>>();

        Ok(output_records)
    }

    fn get_left_matching_count(
        &mut self,
        action: &JoinAction,
        record: &Record,
    ) -> JoinResult<usize> {
        let join_key = get_record_key(record, &self.right_table.join_key_indexes);

        let mut matching_count = self.left_table.count_records_from_join_key(join_key)?;
        if action == &JoinAction::Insert {
            matching_count -= 1;
        }
        Ok(matching_count)
    }

    fn get_right_matching_count(
        &mut self,
        action: &JoinAction,
        record: &Record,
    ) -> JoinResult<usize> {
        let join_key = get_record_key(record, &self.left_table.join_key_indexes);

        let mut matching_count = self.right_table.count_records_from_join_key(join_key)?;
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
                let join_key = get_record_key(old, &self.left_table.join_key_indexes);

                self.left_table.remove_record(old)?;

                let records = self.inner_join_from_left(&JoinAction::Delete, join_key, old)?;
                Ok(records)
            }
            (JoinType::Inner, JoinBranch::Right) => {
                let join_key = get_record_key(old, &self.right_table.join_key_indexes);

                self.right_table.remove_record(old)?;

                let records = self.inner_join_from_right(&JoinAction::Delete, join_key, old)?;
                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Left) => {
                let join_key = get_record_key(old, &self.left_table.join_key_indexes);
                self.left_table.remove_record(old)?;
                let records = self.left_join_from_left(&JoinAction::Delete, join_key, old)?;
                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Right) => {
                let join_key = get_record_key(old, &self.right_table.join_key_indexes);
                self.right_table.remove_record(old)?;
                let records = self.left_join_from_right(&JoinAction::Delete, join_key, old)?;
                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Left) => {
                let join_key = get_record_key(old, &self.left_table.join_key_indexes);
                self.left_table.remove_record(old)?;
                let records = self.right_join_from_left(&JoinAction::Delete, join_key, old)?;
                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Right) => {
                let join_key = get_record_key(old, &self.right_table.join_key_indexes);
                self.right_table.remove_record(old)?;
                let records = self.right_join_from_right(&JoinAction::Delete, join_key, old)?;
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
                let join_key = get_record_key(new, &self.left_table.join_key_indexes);

                self.left_table.add_record(join_key, new)?;

                let records = self.inner_join_from_left(&JoinAction::Insert, join_key, new)?;
                Ok(records)
            }
            (JoinType::Inner, JoinBranch::Right) => {
                let join_key = get_record_key(new, &self.right_table.join_key_indexes);

                self.right_table.add_record(join_key, new)?;

                let records = self.inner_join_from_right(&JoinAction::Insert, join_key, new)?;

                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Left) => {
                let join_key = get_record_key(new, &self.left_table.join_key_indexes);

                self.left_table.add_record(join_key, new)?;

                let records = self.left_join_from_left(&JoinAction::Insert, join_key, new)?;

                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Right) => {
                let join_key = get_record_key(new, &self.right_table.join_key_indexes);

                self.right_table.add_record(join_key, new)?;

                let records = self.left_join_from_right(&JoinAction::Insert, join_key, new)?;

                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Left) => {
                let join_key = get_record_key(new, &self.left_table.join_key_indexes);

                self.left_table.add_record(join_key, new)?;

                let records = self.right_join_from_left(&JoinAction::Insert, join_key, new)?;

                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Right) => {
                let join_key = get_record_key(new, &self.right_table.join_key_indexes);

                self.right_table.add_record(join_key, new)?;

                let records = self.right_join_from_right(&JoinAction::Insert, join_key, new)?;

                Ok(records)
            }
        }
    }

    pub fn commit(&mut self) -> Result<(), StorageError> {
        self.left_table.record_key_to_record.flush()?;
        self.right_table.record_key_to_record.flush()
    }
}

#[derive(Debug)]
struct TableForJoin {
    join_key_indexes: Vec<usize>,
    primary_key_indexes: Vec<usize>,
    default_record: Record,
    join_key_to_record_key: HashMap<u64, Vec<u64>>,
    record_key_to_record: RocksdbMap<u64, Record>,
}

impl TableForJoin {
    fn new(
        join_key_indexes: Vec<usize>,
        primary_key_indexes: Vec<usize>,
        default_record: Record,
        db_path: &str,
    ) -> Result<Self, StorageError> {
        let record_key_to_record = RocksdbMap::<u64, Record>::create(db_path.as_ref())?;
        Ok(Self {
            join_key_indexes,
            primary_key_indexes,
            default_record,
            join_key_to_record_key: Default::default(),
            record_key_to_record,
        })
    }

    fn lookup_size(&self) -> Result<usize, StorageError> {
        self.record_key_to_record.count()
    }

    fn get_records_from_join_key(&self, join_key: u64) -> Result<Vec<Record>, StorageError> {
        let mut result = vec![];
        if let Some(record_keys) = self.join_key_to_record_key.get(&join_key) {
            for record_key in record_keys {
                if let Some(record) = self.record_key_to_record.get(record_key)? {
                    result.push(record.into_owned());
                }
            }
        }
        Ok(result)
    }

    fn count_records_from_join_key(&self, join_key: u64) -> Result<usize, StorageError> {
        let mut result = 0;
        if let Some(record_keys) = self.join_key_to_record_key.get(&join_key) {
            for record_key in record_keys {
                if self.record_key_to_record.contains(record_key)? {
                    result += 1;
                }
            }
        }
        Ok(result)
    }

    fn remove_record(&self, record: &Record) -> Result<(), StorageError> {
        let primary_key = get_record_key(record, &self.primary_key_indexes);
        self.record_key_to_record.remove(&primary_key)
    }

    fn add_record(&mut self, join_key: u64, record: &Record) -> Result<(), StorageError> {
        let primary_key = get_record_key(record, &self.primary_key_indexes);
        match self.join_key_to_record_key.entry(join_key) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().push(primary_key);
            }
            Entry::Vacant(entry) => {
                entry.insert(vec![primary_key]);
            }
        }
        self.record_key_to_record.insert(&primary_key, &record)
    }
}

fn get_record_key(record: &Record, key_indexes: &[usize]) -> u64 {
    let mut hasher = AHasher::default();
    for index in key_indexes.iter() {
        record.values[*index].hash(&mut hasher);
    }
    hasher.finish()
}

fn join_records(left_record: Record, right_record: Record) -> Record {
    let mut concat_values = left_record.values;
    concat_values.extend(right_record.values);
    let mut output_record = Record::new(None, concat_values);

    if let Some(left_record_lifetime) = left_record.lifetime {
        if let Some(right_record_lifetime) = right_record.lifetime {
            if left_record_lifetime.reference > right_record_lifetime.reference {
                output_record.set_lifetime(Some(left_record_lifetime));
            } else {
                output_record.set_lifetime(Some(right_record_lifetime));
            }
        } else {
            output_record.set_lifetime(Some(left_record_lifetime));
        }
    } else if let Some(right_record_lifetime) = right_record.lifetime {
        output_record.set_lifetime(Some(right_record_lifetime));
    }

    output_record
}
