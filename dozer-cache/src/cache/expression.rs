use dozer_types::types::{Field, IndexDefinition};
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

#[derive(Clone, Debug, PartialEq)]
pub enum Operator {
    LT,
    LTE,
    EQ,
    GT,
    GTE,
    Contains,
    MatchesAny,
    MatchesAll,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SortDirection {
    Ascending,
    Descending,
}
#[derive(Clone, Debug, PartialEq)]
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
#[derive(Clone, Debug, PartialEq)]
pub struct SeqScan {
    // ascending / descending
    pub direction: bool,
}
