use dozer_types::serde::{self, Deserialize, Serialize};
use dozer_types::serde_json::Value;
use dozer_types::types::{IndexDefinition, SortDirection};
mod query_helper;
mod query_serde;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(crate = "self::serde")]
pub struct QueryExpression {
    /// Final results must pass all the filters.
    #[serde(rename = "$filter", default)]
    pub filters: Vec<FilterExpression>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterExpression {
    // a = 1, a containts "s", a> 4
    pub field_name: String,
    pub operator: Operator,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

impl Operator {
    pub fn convert_str(s: &str) -> Option<Operator> {
        match s {
            "$lt" => Some(Operator::LT),
            "$lte" => Some(Operator::LTE),
            "$gt" => Some(Operator::GT),
            "$gte" => Some(Operator::GTE),
            "$eq" => Some(Operator::EQ),
            "$contains" => Some(Operator::Contains),
            "$matches_any" => Some(Operator::MatchesAny),
            "$matches_all" => Some(Operator::MatchesAll),
            _ => None,
        }
    }
    pub fn to_str(&self) -> &'static str {
        match self {
            Operator::LT => "$lt",
            Operator::LTE => "$lte",
            Operator::EQ => "$eq",
            Operator::GT => "$gt",
            Operator::GTE => "$gte",
            Operator::Contains => "$contains",
            Operator::MatchesAny => "$matches_any",
            Operator::MatchesAll => "$matches_all",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(crate = "self::serde")]
pub struct SortOptions {
    pub field_name: String,
    pub direction: SortDirection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexScan {
    pub index_def: IndexDefinition,
    pub filters: Vec<FilterExpression>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeqScan {
    pub direction: SortDirection,
}

pub enum PlanningResult {
    /// These indices are needed to execute this query, but not present in the schema.
    NeedIndices(Vec<IndexDefinition>),
    /// The query should be carried out with multiple `IndexScan`s and results are the intersection of all `IndexScan` results.
    IndexScans(Vec<IndexScan>),
    /// Just split out the results in specified direction.
    SeqScan(SeqScan),
}
