mod helper;
mod planner;
use dozer_types::types::{Field, SortDirection};
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
    pub index_id: usize,
    pub kind: IndexScanKind,
}

#[derive(Clone, Debug, PartialEq)]
pub enum IndexScanKind {
    SortedInverted {
        eq_filters: Vec<IndexFilter>,
        range_query: Option<RangeQuery>,
    },
    FullText {
        filter: IndexFilter,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeqScan {
    pub direction: SortDirection,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IndexFilter {
    pub field_index: usize,
    pub op: Operator,
    pub val: Field,
}

impl IndexFilter {
    pub fn new(field_index: usize, op: Operator, val: Field) -> Self {
        Self {
            field_index,
            op,
            val,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RangeQuery {
    pub field_index: usize,
    pub operator_and_value: Option<(Operator, Field)>,
}
