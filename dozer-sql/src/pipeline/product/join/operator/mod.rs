use dozer_core::processor_record::{ProcessorRecord, ProcessorRecordStore};
use dozer_types::types::{Record, Schema, Timestamp};

use crate::pipeline::errors::JoinError;

use self::table::{JoinKey, JoinTable};

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
        let join_records = create_join_records_fn(record, record_branch);

        table
            .get_matching_records(join_key, default_if_no_match)
            .map(|matching_record| (action, join_records(matching_record)))
            .collect()
    }

    fn outer_join(
        &self,
        action: JoinAction,
        join_key: &JoinKey,
        record: &ProcessorRecord,
        record_branch: JoinBranch,
    ) -> Vec<(JoinAction, ProcessorRecord)> {
        let (table_to_match, matching_record_branch, table_of_record) = match record_branch {
            JoinBranch::Left => (&self.right, JoinBranch::Right, &self.left),
            JoinBranch::Right => (&self.left, JoinBranch::Left, &self.right),
        };
        let join_records = create_join_records_fn(record, record_branch);

        // We need to query from the table where this record is from:
        // - For JoinAction::Insert, did this join key exist before this insert? If not, we need to remove the default record.
        // - For JoinAction::Delete, does this join key exist after this delete? If not, we need to insert the default record.
        let need_to_act_on_default_record = match action {
            JoinAction::Insert => {
                // Because this record is already inserted, the join key didn't exist before this insert iif the matching count is now 1.
                table_of_record
                    .get_matching_records(join_key, false)
                    .take(2)
                    .count()
                    == 1
            }
            JoinAction::Delete => {
                table_of_record
                    .get_matching_records(join_key, false)
                    .take(1)
                    .count()
                    == 0
            }
        };

        let mut output_records = vec![];
        for matching_record in table_to_match.get_matching_records(join_key, false) {
            let join_record = join_records(matching_record);

            if need_to_act_on_default_record {
                let default_join_record = create_join_records_fn(
                    matching_record,
                    matching_record_branch,
                )(table_of_record.default_record());
                match action {
                    JoinAction::Insert => {
                        // delete the default join record
                        output_records.push((JoinAction::Delete, default_join_record));
                        // insert the new join record
                        output_records.push((JoinAction::Insert, join_record));
                    }
                    JoinAction::Delete => {
                        output_records.push((JoinAction::Delete, join_record));
                        output_records.push((JoinAction::Insert, default_join_record));
                    }
                }
            } else {
                output_records.push((action, join_record));
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
                self.outer_join(action, join_key, record, JoinBranch::Right)
            }
            (JoinType::RightOuter, JoinBranch::Left) => {
                self.outer_join(action, join_key, record, JoinBranch::Left)
            }
            (JoinType::RightOuter, JoinBranch::Right) => {
                self.inner_join(action, join_key, record, JoinBranch::Right, true)
            }
        }
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

fn create_join_records_fn(
    record: &ProcessorRecord,
    record_branch: JoinBranch,
) -> impl Fn(&ProcessorRecord) -> ProcessorRecord + '_ {
    let lifetime = record.get_lifetime();
    move |matching_record| {
        let record = record.clone();
        let matching_lifetime = matching_record.get_lifetime();
        let matching_record = matching_record.clone();

        let mut output_record = ProcessorRecord::new();
        match record_branch {
            JoinBranch::Left => {
                output_record.extend(record);
                output_record.extend(matching_record);
            }
            JoinBranch::Right => {
                output_record.extend(matching_record);
                output_record.extend(record);
            }
        }

        if let Some(lifetime) = &lifetime {
            if let Some(matching_lifetime) = matching_lifetime {
                if lifetime.reference > matching_lifetime.reference {
                    output_record.set_lifetime(Some(lifetime.clone()));
                } else {
                    output_record.set_lifetime(Some(matching_lifetime));
                }
            } else {
                output_record.set_lifetime(Some(lifetime.clone()));
            }
        } else if let Some(matching_lifetime) = matching_lifetime {
            output_record.set_lifetime(Some(matching_lifetime));
        }

        output_record
    }
}
