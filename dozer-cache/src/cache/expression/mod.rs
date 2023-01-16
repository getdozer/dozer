use dozer_types::serde::{self, Deserialize, Serialize};
use dozer_types::serde_json::Value;
mod query_helper;
mod query_serde;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
pub struct QueryExpression {
    #[serde(rename = "$filter", default)]
    pub filter: Option<FilterExpression>,
    #[serde(rename = "$order_by", default)]
    pub order_by: SortOptions,
    #[serde(rename = "$limit")]
    pub limit: Option<usize>,
    #[serde(rename = "$skip", default)]
    pub skip: usize,
}
pub fn default_limit_for_query() -> usize {
    50
}
impl Default for QueryExpression {
    fn default() -> Self {
        Self {
            filter: None,
            order_by: Default::default(),
            limit: Some(default_limit_for_query()),
            skip: Default::default(),
        }
    }
}

impl QueryExpression {
    pub fn new(
        filter: Option<FilterExpression>,
        order_by: Vec<SortOption>,
        limit: Option<usize>,
        skip: usize,
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
#[serde(crate = "self::serde")]
pub enum SortDirection {
    Ascending,
    Descending,
}

impl SortDirection {
    pub fn convert_str(s: &str) -> Option<Self> {
        match s {
            "asc" => Some(SortDirection::Ascending),
            "desc" => Some(SortDirection::Descending),
            _ => None,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            SortDirection::Ascending => "asc",
            SortDirection::Descending => "desc",
        }
    }
}
