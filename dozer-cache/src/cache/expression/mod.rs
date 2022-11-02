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
    Simple(String, Operator, Value),
    And(Vec<FilterExpression>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

    pub fn supported_by_sorted_inverted(&self) -> bool {
        match self {
            Operator::LT | Operator::LTE | Operator::EQ | Operator::GT | Operator::GTE => true,
            Operator::Contains | Operator::MatchesAny | Operator::MatchesAll => false,
        }
    }

    pub fn supported_by_full_text(&self) -> bool {
        match self {
            Operator::LT | Operator::LTE | Operator::EQ | Operator::GT | Operator::GTE => false,
            Operator::Contains | Operator::MatchesAny | Operator::MatchesAll => true,
        }
    }

    pub fn is_range_operator(&self) -> bool {
        match self {
            Operator::LT | Operator::LTE | Operator::GT | Operator::GTE => true,
            Operator::EQ | Operator::Contains | Operator::MatchesAny | Operator::MatchesAll => {
                false
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(crate = "self::serde")]
pub struct SortOptions {
    pub field_name: String,
    pub direction: SortDirection,
}

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
