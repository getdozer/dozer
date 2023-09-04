use dozer_core::processor_record::{ProcessorRecord, ProcessorRecordStore};
use dozer_types::types::{Record, Schema, Timestamp};

use crate::pipeline::errors::JoinError;

use self::table::{join_records, JoinKey, JoinTable};

use super::JoinResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinAction {
    Insert,
    Delete,
}

mod table;

#[derive(Debug, Clone)]
pub struct JoinOperator {
    join_type: JoinType,

    left: JoinTable,
    right: JoinTable,
}

impl JoinOperator {
    pub fn new(
        join_type: JoinType,
        (left_join_key_indexes, right_join_key_indexes): (Vec<usize>, Vec<usize>),
        (left_schema, right_schema): (&Schema, &Schema),
        record_store: &ProcessorRecordStore,
        enable_probabilistic_optimizations: bool,
    ) -> Result<Self, JoinError> {
        let accurate_keys = !enable_probabilistic_optimizations;
        let left = JoinTable::new(
            left_schema,
            left_join_key_indexes,
            record_store,
            accurate_keys,
        )?;
        let right = JoinTable::new(
            right_schema,
            right_join_key_indexes,
            record_store,
            accurate_keys,
        )?;
        Ok(Self {
            join_type,
            left,
            right,
        })
    }

    pub fn left_lookup_size(&self) -> usize {
        self.left.lookup_size()
    }

    pub fn right_lookup_size(&self) -> usize {
        self.right.lookup_size()
    }

    fn inner_join(
        &self,
        action: JoinAction,
        join_key: &JoinKey,
        record: &ProcessorRecord,
        record_branch: JoinBranch,
        default_if_no_match: bool,
    ) -> Vec<(JoinAction, ProcessorRecord)> {
        let table = match record_branch {
            JoinBranch::Left => &self.right,
            JoinBranch::Right => &self.left,
        };

        table
            .get_join_records(join_key, record, record_branch, default_if_no_match)
            .map(|record| (action, record))
            .collect()
    }

    fn left_join_from_right(
        &self,
        action: JoinAction,
        join_key: &JoinKey,
        right_record: &ProcessorRecord,
    ) -> Vec<(JoinAction, ProcessorRecord)> {
        let left_records = self.left.get_matching_records(join_key, false);

        let mut output_records = vec![];

        for left_record in left_records {
            let right_matching_count = self.get_right_matching_count(action, join_key);
            let join_record = join_records(left_record.clone(), right_record.clone());

            if right_matching_count > 0 {
                // if there are multiple matching records on the right branch, the left record will be just returned
                output_records.push((action, join_record));
            } else {
                let default_join_record =
                    join_records(left_record.clone(), self.right.default_record().clone());
                match action {
                    JoinAction::Insert => {
                        // delete the "first left join" record
                        output_records.push((JoinAction::Delete, default_join_record));
                        // insert the new left join record
                        output_records.push((JoinAction::Insert, join_record));
                    }
                    JoinAction::Delete => {
                        output_records.push((JoinAction::Delete, join_record));
                        output_records.push((JoinAction::Insert, default_join_record));
                    }
                }
            }
        }
        output_records
    }

    fn right_join_from_left(
        &self,
        action: JoinAction,
        join_key: &JoinKey,
        left_record: &ProcessorRecord,
    ) -> Vec<(JoinAction, ProcessorRecord)> {
        let right_records = self.right.get_matching_records(join_key, false);

        let mut output_records = vec![];

        for right_record in right_records {
            let left_matching_count = self.get_left_matching_count(action, join_key);
            let join_record = join_records(left_record.clone(), right_record.clone());

            if left_matching_count > 0 {
                // if there are multiple matching records on the left branch, the right record will be just returned
                output_records.push((action, join_record));
            } else {
                let default_join_record =
                    join_records(self.left.default_record().clone(), right_record.clone());
                match action {
                    JoinAction::Insert => {
                        // delete the "first left join" record
                        output_records.push((JoinAction::Delete, default_join_record));
                        // insert the new left join record
                        output_records.push((JoinAction::Insert, join_record));
                    }
                    JoinAction::Delete => {
                        output_records.push((JoinAction::Delete, join_record));
                        output_records.push((JoinAction::Insert, default_join_record));
                    }
                }
            }
        }
        output_records
    }

    fn join(
        &self,
        action: JoinAction,
        join_key: &JoinKey,
        record: &ProcessorRecord,
        record_branch: JoinBranch,
    ) -> Vec<(JoinAction, ProcessorRecord)> {
        match (&self.join_type, record_branch) {
            (JoinType::Inner, _) => self.inner_join(action, join_key, record, record_branch, false),
            (JoinType::LeftOuter, JoinBranch::Left) => {
                self.inner_join(action, join_key, record, JoinBranch::Left, true)
            }
            (JoinType::LeftOuter, JoinBranch::Right) => {
                self.left_join_from_right(action, join_key, record)
            }
            (JoinType::RightOuter, JoinBranch::Left) => {
                self.right_join_from_left(action, join_key, record)
            }
            (JoinType::RightOuter, JoinBranch::Right) => {
                self.inner_join(action, join_key, record, JoinBranch::Right, true)
            }
        }
    }

    fn get_left_matching_count(&self, action: JoinAction, join_key: &JoinKey) -> usize {
        let mut matching_count = self.left.get_matching_records(join_key, false).count();
        if action == JoinAction::Insert {
            matching_count -= 1;
        }
        matching_count
    }

    fn get_right_matching_count(&self, action: JoinAction, join_key: &JoinKey) -> usize {
        let mut matching_count = self.right.get_matching_records(join_key, false).count();
        if action == JoinAction::Insert {
            matching_count -= 1;
        }
        matching_count
    }

    pub fn delete(
        &mut self,
        from: JoinBranch,
        old: &ProcessorRecord,
        old_decoded: &Record,
    ) -> Vec<(JoinAction, ProcessorRecord)> {
        let join_key = match from {
            JoinBranch::Left => self.left.remove(old_decoded),
            JoinBranch::Right => self.right.remove(old_decoded),
        };

        self.join(JoinAction::Delete, &join_key, old, from)
    }

    pub fn insert(
        &mut self,
        from: JoinBranch,
        new: &ProcessorRecord,
        new_decoded: &Record,
    ) -> JoinResult<Vec<(JoinAction, ProcessorRecord)>> {
        let join_key = match from {
            JoinBranch::Left => self.left.insert(new.clone(), new_decoded)?,
            JoinBranch::Right => self.right.insert(new.clone(), new_decoded)?,
        };

        Ok(self.join(JoinAction::Insert, &join_key, new, from))
    }

    pub fn evict_index(&mut self, now: &Timestamp) {
        self.left.evict_index(now);
        self.right.evict_index(now);
    }
}
