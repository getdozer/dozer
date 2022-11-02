mod helper;
mod planner;
use dozer_types::{
    serde_json::Value,
    types::{IndexDefinition, SortDirection},
};
pub use planner::QueryPlanner;

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
    pub fields: Vec<Option<Value>>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeqScan {
    pub direction: SortDirection,
}
