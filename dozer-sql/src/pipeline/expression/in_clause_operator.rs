//! Streaming SQL `IN` and `NOT IN` Expression Evaluator for Dozer Core (`dozer-sql`)
//! Bounty: getdozer/dozer #1659 ($600 USD — Support for IN Clause in Streaming SQL)

use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarFieldValue {
    Null,
    Bool(bool),
    Int64(i64),
    String(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamOperation {
    Insert { row: Vec<ScalarFieldValue> },
    Delete { row: Vec<ScalarFieldValue> },
    Update { old: Vec<ScalarFieldValue>, new: Vec<ScalarFieldValue> },
}

#[derive(Debug, Clone)]
pub struct InListFilter {
    pub column_index: usize,
    pub negated: bool,
    pub non_null_set: HashSet<ScalarFieldValue>,
    pub has_null_in_list: bool,
}

impl InListFilter {
    pub fn new(column_index: usize, list: Vec<ScalarFieldValue>, negated: bool) -> Self {
        let mut non_null_set = HashSet::new();
        let mut has_null_in_list = false;
        for item in list {
            if item == ScalarFieldValue::Null {
                has_null_in_list = true;
            } else {
                non_null_set.insert(item);
            }
        }
        Self {
            column_index,
            negated,
            non_null_set,
            has_null_in_list,
        }
    }

    pub fn matches_row(&self, row: &[ScalarFieldValue]) -> bool {
        let Some(candidate) = row.get(self.column_index) else {
            return false;
        };
        if *candidate == ScalarFieldValue::Null {
            return false;
        }

        let contains = self.non_null_set.contains(candidate);
        if !self.negated {
            contains
        } else {
            if contains || self.has_null_in_list {
                false
            } else {
                true
            }
        }
    }

    pub fn process_stream_op(&self, op: StreamOperation) -> Option<StreamOperation> {
        match op {
            StreamOperation::Insert { row } => {
                self.matches_row(&row).then_some(StreamOperation::Insert { row })
            }
            StreamOperation::Delete { row } => {
                self.matches_row(&row).then_some(StreamOperation::Delete { row })
            }
            StreamOperation::Update { old, new } => {
                let old_matched = self.matches_row(&old);
                let new_matched = self.matches_row(&new);
                match (old_matched, new_matched) {
                    (true, true) => Some(StreamOperation::Update { old, new }),
                    (false, true) => Some(StreamOperation::Insert { row: new }),
                    (true, false) => Some(StreamOperation::Delete { row: old }),
                    (false, false) => None,
                }
            }
        }
    }
}
