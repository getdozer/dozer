use dozer_types::types::{Field, IndexDefinition};
use serde::Deserialize;
use strum_macros::EnumString;
mod deserializer;
mod query_helper;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Deserialize, Default)]
pub struct QueryExpression {
    #[serde(rename = "$filter", default)]
    pub filter: Option<FilterExpression>,
    #[serde(rename = "$order_by", default)]
    pub order_by: Vec<SortOptions>,
    #[serde(rename = "$limit", default = "default_limit")]
    pub limit: usize,
    #[serde(rename = "$skip", default)]
    pub skip: usize,
}
fn default_limit() -> usize {
    50
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

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub enum SortDirection {
    #[serde(rename = "asc")]
    Ascending,
    #[serde(rename = "desc")]
    Descending,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
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
