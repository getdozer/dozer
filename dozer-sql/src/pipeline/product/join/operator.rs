use ahash::AHasher;
use dozer_storage::{
    errors::StorageError,
    lmdb::{RwTransaction, Transaction},
    lmdb_storage::{LmdbEnvironmentManager, LmdbEnvironmentOptions},
    LmdbMap, LmdbMultimap, RwLmdbEnvironment,
};
use dozer_types::{
    borrow::{Borrow, IntoOwned},
    types::Record,
};
use std::{
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
    environment: RwLmdbEnvironment,

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
        name: &str,
    ) -> JoinResult<Self> {
        let temp_dir = TempDir::new("join").map_err(JoinError::CreateTempDir)?;
        let mut environment = LmdbEnvironmentManager::create_rw(
            temp_dir.path(),
            name,
            LmdbEnvironmentOptions {
                max_map_sz: 1024 * 1024 * 1024 * 1024,
                ..Default::default()
            },
        )?;

        let left_table = TableForJoin::new(
            left_join_key_indexes,
            left_primary_key_indexes,
            left_default_record,
            &mut environment,
            "left",
        )?;
        let right_table = TableForJoin::new(
            right_join_key_indexes,
            right_primary_key_indexes,
            right_default_record,
            &mut environment,
            "right",
        )?;

        Ok(Self {
            join_type,
            _temp_dir: temp_dir,
            environment,
            left_table,
            right_table,
        })
    }

    pub fn left_lookup_size(&mut self) -> Result<usize, StorageError> {
        self.left_table.lookup_size(self.environment.txn_mut()?)
    }

    pub fn right_lookup_size(&mut self) -> Result<usize, StorageError> {
        self.right_table.lookup_size(self.environment.txn_mut()?)
    }

    fn inner_join_from_left(
        &mut self,
        action: &JoinAction,
        join_key: u64,
        left_record: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let right_records = self
            .right_table
            .get_records_from_join_key(self.environment.txn_mut()?, join_key)?;

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
        let left_records = self
            .left_table
            .get_records_from_join_key(self.environment.txn_mut()?, join_key)?;

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
        let right_records = self
            .right_table
            .get_records_from_join_key(self.environment.txn_mut()?, join_key)?;

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
        let left_records = self
            .left_table
            .get_records_from_join_key(self.environment.txn_mut()?, join_key)?;

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
        let right_records = self
            .right_table
            .get_records_from_join_key(self.environment.txn_mut()?, join_key)?;

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
        let left_records = self
            .left_table
            .get_records_from_join_key(self.environment.txn_mut()?, join_key)?;

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

        let mut matching_count = self
            .left_table
            .count_records_from_join_key(self.environment.txn_mut()?, join_key)?;
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

        let mut matching_count = self
            .right_table
            .count_records_from_join_key(self.environment.txn_mut()?, join_key)?;
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

                self.left_table
                    .remove_record(self.environment.txn_mut()?, old)?;

                let records = self.inner_join_from_left(&JoinAction::Delete, join_key, old)?;
                Ok(records)
            }
            (JoinType::Inner, JoinBranch::Right) => {
                let join_key = get_record_key(old, &self.right_table.join_key_indexes);

                self.right_table
                    .remove_record(self.environment.txn_mut()?, old)?;

                let records = self.inner_join_from_right(&JoinAction::Delete, join_key, old)?;
                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Left) => {
                let join_key = get_record_key(old, &self.left_table.join_key_indexes);
                self.left_table
                    .remove_record(self.environment.txn_mut()?, old)?;
                let records = self.left_join_from_left(&JoinAction::Delete, join_key, old)?;
                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Right) => {
                let join_key = get_record_key(old, &self.right_table.join_key_indexes);
                self.right_table
                    .remove_record(self.environment.txn_mut()?, old)?;
                let records = self.left_join_from_right(&JoinAction::Delete, join_key, old)?;
                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Left) => {
                let join_key = get_record_key(old, &self.left_table.join_key_indexes);
                self.left_table
                    .remove_record(self.environment.txn_mut()?, old)?;
                let records = self.right_join_from_left(&JoinAction::Delete, join_key, old)?;
                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Right) => {
                let join_key = get_record_key(old, &self.right_table.join_key_indexes);
                self.right_table
                    .remove_record(self.environment.txn_mut()?, old)?;
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

                self.left_table
                    .add_record(self.environment.txn_mut()?, join_key, new)?;

                let records = self.inner_join_from_left(&JoinAction::Insert, join_key, new)?;
                Ok(records)
            }
            (JoinType::Inner, JoinBranch::Right) => {
                let join_key = get_record_key(new, &self.right_table.join_key_indexes);

                self.right_table
                    .add_record(self.environment.txn_mut()?, join_key, new)?;

                let records = self.inner_join_from_right(&JoinAction::Insert, join_key, new)?;

                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Left) => {
                let join_key = get_record_key(new, &self.left_table.join_key_indexes);

                self.left_table
                    .add_record(self.environment.txn_mut()?, join_key, new)?;

                let records = self.left_join_from_left(&JoinAction::Insert, join_key, new)?;

                Ok(records)
            }
            (JoinType::LeftOuter, JoinBranch::Right) => {
                let join_key = get_record_key(new, &self.right_table.join_key_indexes);

                self.right_table
                    .add_record(self.environment.txn_mut()?, join_key, new)?;

                let records = self.left_join_from_right(&JoinAction::Insert, join_key, new)?;

                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Left) => {
                let join_key = get_record_key(new, &self.left_table.join_key_indexes);

                self.left_table
                    .add_record(self.environment.txn_mut()?, join_key, new)?;

                let records = self.right_join_from_left(&JoinAction::Insert, join_key, new)?;

                Ok(records)
            }
            (JoinType::RightOuter, JoinBranch::Right) => {
                let join_key = get_record_key(new, &self.right_table.join_key_indexes);

                self.right_table
                    .add_record(self.environment.txn_mut()?, join_key, new)?;

                let records = self.right_join_from_right(&JoinAction::Insert, join_key, new)?;

                Ok(records)
            }
        }
    }

    pub fn commit(&mut self) -> Result<(), StorageError> {
        self.environment.commit()
    }
}

#[derive(Debug)]
struct TableForJoin {
    join_key_indexes: Vec<usize>,
    primary_key_indexes: Vec<usize>,
    default_record: Record,
    join_key_to_record_key: LmdbMultimap<u64, u64>,
    record_key_to_records: LmdbMap<u64, Record>,
}

impl TableForJoin {
    fn new(
        join_key_indexes: Vec<usize>,
        primary_key_indexes: Vec<usize>,
        default_record: Record,
        environment: &mut RwLmdbEnvironment,
        database_name_prefix: &str,
    ) -> Result<Self, StorageError> {
        let join_key_to_record_key = LmdbMultimap::create(
            environment,
            Some(&format!("{}-join-key-to-primary-key", database_name_prefix)),
        )?;
        let record_key_to_records = LmdbMap::create(
            environment,
            Some(&format!("{}-primary-key-to-records", database_name_prefix)),
        )?;
        Ok(Self {
            join_key_indexes,
            primary_key_indexes,
            default_record,
            join_key_to_record_key,
            record_key_to_records,
        })
    }

    fn lookup_size<T: Transaction>(&self, txn: &T) -> Result<usize, StorageError> {
        self.record_key_to_records.count(txn)
    }

    fn get_records_from_join_key<T: Transaction>(
        &self,
        txn: &T,
        join_key: u64,
    ) -> Result<Vec<Record>, StorageError> {
        let mut result = vec![];
        for primary_key in self.join_key_to_record_key.iter_dup(txn, &join_key)? {
            let primary_key = primary_key?;
            if let Some(record) = self.record_key_to_records.get(txn, primary_key.borrow())? {
                result.push(record.into_owned());
            }
        }
        Ok(result)
    }

    fn count_records_from_join_key<T: Transaction>(
        &self,
        txn: &T,
        join_key: u64,
    ) -> Result<usize, StorageError> {
        let mut result = 0;
        for primary_key in self.join_key_to_record_key.iter_dup(txn, &join_key)? {
            let primary_key = primary_key?;
            if self
                .record_key_to_records
                .contains(txn, primary_key.borrow())?
            {
                result += 1;
            }
        }
        Ok(result)
    }

    fn remove_record(&self, txn: &mut RwTransaction, record: &Record) -> Result<(), JoinError> {
        let primary_key = get_record_key(record, &self.primary_key_indexes);
        if !self.record_key_to_records.remove(txn, &primary_key)? {
            return Err(JoinError::MissingPrimaryKey {
                primary_indexes: self.primary_key_indexes.clone(),
                record: record.clone(),
                key: primary_key,
            });
        }
        Ok(())
    }

    fn add_record(
        &self,
        txn: &mut RwTransaction,
        join_key: u64,
        record: &Record,
    ) -> Result<(), JoinError> {
        let primary_key = get_record_key(record, &self.primary_key_indexes);
        self.join_key_to_record_key
            .insert(txn, &join_key, &primary_key)?;
        if !self
            .record_key_to_records
            .insert(txn, &primary_key, record)?
        {
            return Err(JoinError::DuplicatePrimaryKey {
                primary_indexes: self.primary_key_indexes.clone(),
                record: record.clone(),
                key: primary_key,
            });
        }
        Ok(())
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
