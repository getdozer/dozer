use dozer_types::types::{Record, Schema, Timestamp};

use crate::errors::JoinError;

use self::table::{JoinKey, JoinTable};

use super::JoinResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinBranch {
    Left,
    Right,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    LeftSemi,
    LeftAnti,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinAction {
    Insert,
    Delete,
}

mod table;

struct JoinStep<'a> {
    action: JoinAction,
    join_key: &'a JoinKey,
    record: &'a Record,
    record_branch: JoinBranch,
}

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
        enable_probabilistic_optimizations: bool,
    ) -> Result<Self, JoinError> {
        let accurate_keys = !enable_probabilistic_optimizations;
        let left = JoinTable::new(left_schema, left_join_key_indexes, accurate_keys)?;
        let right = JoinTable::new(right_schema, right_join_key_indexes, accurate_keys)?;
        Ok(Self {
            join_type,
            left,
            right,
        })
    }

    // Identify whether this join operator represents an IN / NOT IN subquery
    // semi-join or anti-join so UPDATE transitions preserve single-stream semantics.
    pub fn is_semi_or_anti(&self) -> bool {
        matches!(self.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
    }

    fn inner_join(
        &self,
        step: &JoinStep<'_>,
        default_if_no_match: bool,
    ) -> Vec<(JoinAction, Record)> {
        let table = match step.record_branch {
            JoinBranch::Left => &self.right,
            JoinBranch::Right => &self.left,
        };
        let join_records = create_join_records_fn(step.record, step.record_branch);

        table
            .get_matching_records(step.join_key, default_if_no_match)
            .map(|matching_record| (step.action, join_records(matching_record)))
            .collect()
    }

    fn outer_join(&self, step: &JoinStep<'_>) -> Vec<(JoinAction, Record)> {
        let (table_to_match, table_of_record) = match step.record_branch {
            JoinBranch::Left => (&self.right, &self.left),
            JoinBranch::Right => (&self.left, &self.right),
        };
        let join_records = create_join_records_fn(step.record, step.record_branch);
        let default_join_records =
            create_join_records_fn(table_of_record.default_record(), step.record_branch);
        let act_on_default = should_act_on_default_record(table_of_record, step.action, step.join_key);

        let mut output_records = vec![];
        for matching_record in table_to_match.get_matching_records(step.join_key, false) {
            let join_record = join_records(matching_record);
            if !act_on_default {
                output_records.push((step.action, join_record));
                continue;
            }
            let default_join_record = default_join_records(matching_record);
            emit_outer_transition(&mut output_records, step.action, join_record, default_join_record);
        }

        output_records
    }

    // Evaluate LeftSemi (IN) and LeftAnti (NOT IN) transitions when a record arrives
    // on either the outer parent table (Left) or the inner subquery stream (Right).
    fn semi_or_anti_join(&self, step: &JoinStep<'_>, is_anti: bool) -> Vec<(JoinAction, Record)> {
        match step.record_branch {
            JoinBranch::Left => {
                let has_right_match = self
                    .right
                    .get_matching_records(step.join_key, false)
                    .next()
                    .is_some();
                if has_right_match != is_anti {
                    vec![(step.action, step.record.clone())]
                } else {
                    vec![]
                }
            }
            JoinBranch::Right => {
                if !should_act_on_default_record(&self.right, step.action, step.join_key) {
                    return vec![];
                }
                let emitted_action = resolve_semi_right_action(step.action, is_anti);
                self.left
                    .get_matching_records(step.join_key, false)
                    .map(|left_record| (emitted_action, left_record.clone()))
                    .collect()
            }
        }
    }

    fn join(&self, step: &JoinStep<'_>) -> Vec<(JoinAction, Record)> {
        match (&self.join_type, step.record_branch) {
            (JoinType::Inner, _) => self.inner_join(step, false),
            (JoinType::LeftOuter, JoinBranch::Left) => self.inner_join(step, true),
            (JoinType::LeftOuter, JoinBranch::Right) => self.outer_join(step),
            (JoinType::RightOuter, JoinBranch::Left) => self.outer_join(step),
            (JoinType::RightOuter, JoinBranch::Right) => self.inner_join(step, true),
            (JoinType::LeftSemi, _) => self.semi_or_anti_join(step, false),
            (JoinType::LeftAnti, _) => self.semi_or_anti_join(step, true),
        }
    }

    pub fn delete(
        &mut self,
        from: JoinBranch,
        old: &Record,
        old_decoded: &Record,
    ) -> Vec<(JoinAction, Record)> {
        let join_key = if matches!(from, JoinBranch::Left) {
            self.left.remove(old_decoded)
        } else {
            self.right.remove(old_decoded)
        };
        let step = JoinStep {
            action: JoinAction::Delete,
            join_key: &join_key,
            record: old,
            record_branch: from,
        };
        self.join(&step)
    }

    pub fn insert(
        &mut self,
        from: JoinBranch,
        new: &Record,
        new_decoded: &Record,
    ) -> JoinResult<Vec<(JoinAction, Record)>> {
        let target_table = match from {
            JoinBranch::Left => &mut self.left,
            JoinBranch::Right => &mut self.right,
        };
        let join_key = target_table.insert(new.clone(), new_decoded)?;
        Ok(self.join(&JoinStep {
            action: JoinAction::Insert,
            join_key: &join_key,
            record: new,
            record_branch: from,
        }))
    }

    pub fn evict_index(&mut self, now: &Timestamp) {
        self.left.evict_index(now);
        self.right.evict_index(now);
    }
}

fn should_act_on_default_record(
    table_of_record: &JoinTable,
    action: JoinAction,
    join_key: &JoinKey,
) -> bool {
    match action {
        JoinAction::Insert => {
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
    }
}

fn emit_outer_transition(
    output_records: &mut Vec<(JoinAction, Record)>,
    action: JoinAction,
    join_record: Record,
    default_join_record: Record,
) {
    match action {
        JoinAction::Insert => {
            output_records.push((JoinAction::Delete, default_join_record));
            output_records.push((JoinAction::Insert, join_record));
        }
        JoinAction::Delete => {
            output_records.push((JoinAction::Delete, join_record));
            output_records.push((JoinAction::Insert, default_join_record));
        }
    }
}

fn resolve_semi_right_action(action: JoinAction, is_anti: bool) -> JoinAction {
    match (action, is_anti) {
        (JoinAction::Insert, false) | (JoinAction::Delete, true) => JoinAction::Insert,
        (JoinAction::Delete, false) | (JoinAction::Insert, true) => JoinAction::Delete,
    }
}

fn create_join_records_fn(
    record: &Record,
    record_branch: JoinBranch,
) -> impl Fn(&Record) -> Record + '_ {
    let lifetime = record.get_lifetime();
    move |matching_record| {
        let mut output_record = combine_branch_records(record, matching_record, record_branch);
        let merged_lifetime = merge_record_lifetimes(lifetime.clone(), matching_record.get_lifetime());
        if merged_lifetime.is_some() {
            output_record.set_lifetime(merged_lifetime);
        }
        output_record
    }
}

fn combine_branch_records(
    record: &Record,
    matching_record: &Record,
    record_branch: JoinBranch,
) -> Record {
    let len = record.values().len() + matching_record.values().len();
    let mut data = Vec::with_capacity(len);
    match record_branch {
        JoinBranch::Left => {
            data.extend_from_slice(record.values());
            data.extend_from_slice(matching_record.values());
        }
        JoinBranch::Right => {
            data.extend_from_slice(matching_record.values());
            data.extend_from_slice(record.values());
        }
    }
    Record::new(data)
}

fn merge_record_lifetimes(
    left: Option<dozer_types::types::Lifetime>,
    right: Option<dozer_types::types::Lifetime>,
) -> Option<dozer_types::types::Lifetime> {
    match (left, right) {
        (Some(l), Some(r)) => {
            if l.reference > r.reference {
                Some(l)
            } else {
                Some(r)
            }
        }
        (Some(l), None) => Some(l),
        (None, Some(r)) => Some(r),
        (None, None) => None,
    }
}
