mod helper;
mod planner;
use dozer_types::{
    serde_json::Value,
    types::{IndexDefinition, SortDirection},
};
pub use planner::QueryPlanner;

use super::expression::Operator;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Plan {
    IndexScans(Vec<IndexScan>),
    SeqScan(SeqScan),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexScan {
    pub index_def: IndexDefinition,
    pub index_id: Option<usize>,
    pub filters: Vec<Option<IndexFilter>>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeqScan {
    pub direction: SortDirection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexFilter {
    pub op: Operator,
    pub val: Value,
}

impl IndexFilter {
    pub fn new(op: Operator, val: Value) -> Self {
        Self { op, val }
    }
    pub fn equals(val: Value) -> Self {
        Self {
            op: Operator::EQ,
            val,
        }
    }
}
