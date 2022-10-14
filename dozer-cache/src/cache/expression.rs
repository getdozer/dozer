use dozer_types::types::{Field, IndexDefinition};
use strum_macros::EnumString;

pub struct QueryExpression {
    pub filter: Option<FilterExpression>,
    pub order_by: Vec<SortOptions>,
    pub limit: usize,
    pub skip: usize,
}

impl QueryExpression {
    pub fn new(
        filter: Option<FilterExpression>,
        order_by: Vec<SortOptions>,
        limit: usize,
        skip: usize,
    ) -> Self {
        Self {
            filter,
            order_by,
            limit,
            skip,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FilterExpression {
    // a = 1, a containts "s", a> 4
    Simple(String, Operator, Field),
    And(Box<FilterExpression>, Box<FilterExpression>),
}

#[derive(Clone, Debug, PartialEq, Eq, EnumString, strum_macros::Display)]
pub enum Operator {
    #[strum(serialize = "$lt")]
    LT,
    #[strum(serialize = "$lte")]
    LTE,
    #[strum(serialize = "$eq")]
    EQ,
    #[strum(serialize = "$gt")]
    GT,
    #[strum(serialize = "$gte")]
    GTE,
    #[strum(serialize = "$contains")]
    Contains,
    #[strum(serialize = "$matchesany")]
    MatchesAny,
    #[strum(serialize = "$matchesall")]
    MatchesAll,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortOptions {
    pub field_name: String,
    pub direction: SortDirection,
}

pub enum ExecutionStep {
    IndexScan(IndexScan),
    SeqScan(SeqScan),
}
#[derive(Clone, Debug, PartialEq)]
pub struct IndexScan {
    pub index_def: IndexDefinition,
    pub fields: Vec<Option<Field>>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeqScan {
    // ascending / descending
    pub direction: bool,
}
