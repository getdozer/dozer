mod planner;
pub use planner::QueryPlanner;
mod helper;
use crate::cache::expression::Operator;
use dozer_types::{serde_json::Value, types::IndexDefinition};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimpleFilterExpression {
    pub field_name: String,
    pub operator: Operator,
    pub value: Value,
}

pub enum Plan {
    IndexScans(Vec<IndexScan>),
    /// Just split out the results in specified direction.
    SeqScan(SeqScan),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexScan {
    pub index_def: Vec<(usize, bool)>,
    // pub filters: Vec<SimpleFilterExpression>,
    pub filters: Vec<(usize, Operator, Value)>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeqScan {
    // ascending / descending
    pub direction: bool,
}
