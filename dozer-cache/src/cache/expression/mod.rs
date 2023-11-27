use dozer_types::json_types::JsonValue;
use dozer_types::serde::{Deserialize, Serialize};
mod query_helper;
mod query_serde;
use dozer_types::constants::DEFAULT_DEFAULT_MAX_NUM_RECORDS;
#[cfg(test)]
mod tests;

#[derive(Clone, Debug, Copy, PartialEq)]
pub enum Skip {
    Skip(usize),
    After(u64),
}

impl Default for Skip {
    fn default() -> Self {
        Skip::Skip(0)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryExpression {
    pub filter: Option<FilterExpression>,
    pub order_by: SortOptions,
    pub limit: Option<usize>,
    pub skip: Skip,
}

impl QueryExpression {
    pub fn with_limit(limit: usize) -> Self {
        Self {
            filter: None,
            order_by: Default::default(),
            limit: Some(limit),
            skip: Default::default(),
        }
    }

    pub fn with_no_limit() -> Self {
        Self {
            filter: None,
            order_by: Default::default(),
            limit: None,
            skip: Default::default(),
        }
    }
}

impl Default for QueryExpression {
    fn default() -> Self {
        Self::with_limit(DEFAULT_DEFAULT_MAX_NUM_RECORDS)
    }
}

impl QueryExpression {
    pub fn new(
        filter: Option<FilterExpression>,
        order_by: Vec<SortOption>,
        limit: Option<usize>,
        skip: Skip,
    ) -> Self {
        Self {
            filter,
            order_by: SortOptions(order_by),
            limit,
            skip,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FilterExpression {
    // a = 1, a containts "s", a > 4
    Simple(String, Operator, JsonValue),
    And(Vec<FilterExpression>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub enum Operator {
    #[serde(rename = "$lt")]
    LT,
    #[serde(rename = "$lte")]
    LTE,
    #[serde(rename = "$eq")]
    EQ,
    #[serde(rename = "$gt")]
    GT,
    #[serde(rename = "$gte")]
    GTE,
    #[serde(rename = "$contains")]
    Contains,
    #[serde(rename = "$matches_any")]
    MatchesAny,
    #[serde(rename = "$matches_all")]
    MatchesAll,
}

impl Operator {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortOption {
    pub field_name: String,
    pub direction: SortDirection,
}

impl SortOption {
    pub fn new(field_name: String, direction: SortDirection) -> Self {
        Self {
            field_name,
            direction,
        }
    }
}

/// A wrapper of `Vec<SortOption>`, for customizing the `Serialize` and `Deserialize` implementation.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct SortOptions(pub Vec<SortOption>);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(crate = "dozer_types::serde")]
pub enum SortDirection {
    #[serde(rename = "asc")]
    Ascending,
    #[serde(rename = "desc")]
    Descending,
}

#[derive(Debug, Clone)]
pub struct SQLQuery(pub String);
