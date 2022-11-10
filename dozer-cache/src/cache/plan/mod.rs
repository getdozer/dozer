mod helper;
mod planner;
use dozer_types::types::{Field, IndexDefinition, SortDirection};
pub use planner::QueryPlanner;

use super::expression::Operator;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq)]
pub enum Plan {
    IndexScans(Vec<IndexScan>),
    SeqScan(SeqScan),
}
#[derive(Clone, Debug, PartialEq)]
pub struct IndexScan {
    pub index_def: IndexDefinition,
    pub index_id: Option<usize>,
    pub filters: Vec<Option<IndexFilter>>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeqScan {
    pub direction: SortDirection,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IndexFilter {
    pub op: Operator,
    pub val: Field,
}

impl IndexFilter {
    pub fn new(op: Operator, val: Field) -> Self {
        Self { op, val }
    }
    pub fn equals(val: Field) -> Self {
        Self {
            op: Operator::EQ,
            val,
        }
    }
}
