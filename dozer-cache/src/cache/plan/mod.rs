mod helper;
mod planner;
use dozer_types::types::Field;
pub use planner::QueryPlanner;

use super::expression::{Operator, SortDirection};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Plan {
    IndexScans(Vec<IndexScan>),
    SeqScan(SeqScan),
    ReturnEmpty,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexScan {
    pub index_id: usize,
    pub kind: IndexScanKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexScanKind {
    SortedInverted {
        eq_filters: Vec<(usize, Field)>,
        range_query: Option<SortedInvertedRangeQuery>,
    },
    FullText {
        filter: IndexFilter,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortedInvertedRangeQuery {
    pub field_index: usize,
    pub sort_direction: SortDirection,
    pub operator_and_value: Option<(Operator, Field)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeqScan {
    pub direction: SortDirection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
