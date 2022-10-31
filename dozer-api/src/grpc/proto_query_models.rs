use dozer_cache::cache::{self, expression::QueryExpression};
use dozer_types::{
    serde::{self, Deserialize, Serialize},
    serde_json,
};
impl TryFrom<QueryExpressionRequest> for QueryExpression {
    type Error = crate::errors::GRPCError;
    fn try_from(input: QueryExpressionRequest) -> Result<Self, Self::Error> {
        let mut result = QueryExpression::default();
        result.limit = input.limit.map(|v| v as usize).unwrap_or(result.limit);
        result.skip = input.skip.map(|v| v as usize).unwrap_or(result.skip);
        if let Some(filter_request) = input.filter {
            let filter =
                dozer_cache::cache::expression::FilterExpression::try_from(filter_request)?;
            result.filter = Some(filter);
        }
        if !input.order_by.is_empty() {
            result.order_by = input
                .order_by
                .iter()
                .map(|v| cache::expression::SortOptions::from(v.to_owned()))
                .collect();
        }
        Ok(result)
    }
}
impl TryFrom<SimpleExpression> for dozer_cache::cache::expression::FilterExpression {
    type Error = crate::errors::GRPCError;
    fn try_from(input: SimpleExpression) -> Result<Self, Self::Error> {
        let value = serde_json::to_value(input.to_owned().value.unwrap()).unwrap();
        let input_operator = Operator::from_i32(input.operator).unwrap();
        Ok(dozer_cache::cache::expression::FilterExpression::Simple(
            input.field,
            dozer_cache::cache::expression::Operator::from(input_operator),
            value,
        ))
    }
}

impl From<Operator> for dozer_cache::cache::expression::Operator {
    fn from(input: Operator) -> Self {
        match input {
            Operator::Lt => dozer_cache::cache::expression::Operator::LT,
            Operator::Lte => dozer_cache::cache::expression::Operator::LTE,
            Operator::Eq => dozer_cache::cache::expression::Operator::EQ,
            Operator::Gt => dozer_cache::cache::expression::Operator::GT,
            Operator::Gte => dozer_cache::cache::expression::Operator::GTE,
            Operator::Contains => dozer_cache::cache::expression::Operator::Contains,
            Operator::MatchesAny => dozer_cache::cache::expression::Operator::MatchesAny,
            Operator::MatchesAll => dozer_cache::cache::expression::Operator::MatchesAll,
        }
    }
}

impl TryFrom<FilterExpression> for dozer_cache::cache::expression::FilterExpression {
    type Error = crate::errors::GRPCError;
    fn try_from(input: FilterExpression) -> Result<Self, Self::Error> {
        if let Some(inner_expression) = input.expression {
            match inner_expression {
                Expression::Simple(simple_request) => {
                    let result =
                        dozer_cache::cache::expression::FilterExpression::try_from(simple_request)?;
                    return Ok(result);
                }
                Expression::And(and_expression) => {
                    return dozer_cache::cache::expression::FilterExpression::try_from(
                        and_expression,
                    );
                }
            }
        }
        Err(crate::errors::GRPCError::UnableToDecodeQueryExpression(
            "Inner filter expression is empty".to_owned(),
        ))
    }
}

impl TryFrom<AndExpression> for dozer_cache::cache::expression::FilterExpression {
    type Error = crate::errors::GRPCError;

    fn try_from(input: AndExpression) -> Result<Self, Self::Error> {
        let array_result = input
            .filter_expressions
            .iter()
            .map(|ex| {
                dozer_cache::cache::expression::FilterExpression::try_from(ex.to_owned()).unwrap()
            })
            .collect();
        Ok(dozer_cache::cache::expression::FilterExpression::And(
            array_result,
        ))
    }
}

impl From<SortOptions> for dozer_cache::cache::expression::SortOptions {
    fn from(input: SortOptions) -> Self {
        let sort_direction_input =
            SortDirection::from_i32(input.direction).unwrap_or(SortDirection::Asc);
        dozer_cache::cache::expression::SortOptions {
            field_name: input.field_name,
            direction: dozer_cache::cache::expression::SortDirection::from(sort_direction_input),
        }
    }
}

impl From<SortDirection> for dozer_cache::cache::expression::SortDirection {
    fn from(input: SortDirection) -> Self {
        match input {
            SortDirection::Asc => dozer_cache::cache::expression::SortDirection::Ascending,
            SortDirection::Desc => dozer_cache::cache::expression::SortDirection::Descending,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(crate = "self::serde")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryExpressionRequest {
    #[prost(message, optional, tag = "1")]
    pub filter: ::core::option::Option<FilterExpression>,
    #[prost(message, repeated, tag = "2")]
    pub order_by: ::prost::alloc::vec::Vec<SortOptions>,
    #[prost(uint32, optional, tag = "3")]
    pub limit: ::core::option::Option<u32>,
    #[prost(uint32, optional, tag = "4")]
    pub skip: ::core::option::Option<u32>,
}

#[derive(Serialize, Deserialize)]
#[serde(crate = "self::serde")]
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct SortOptions {
    #[prost(string, tag = "1")]
    pub field_name: ::prost::alloc::string::String,
    #[prost(enumeration = "SortDirection", tag = "2")]
    pub direction: i32,
}
#[derive(Serialize, Deserialize)]
#[serde(crate = "self::serde")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SortDirection {
    Asc = 0,
    Desc = 1,
}
impl SortDirection {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            SortDirection::Asc => "asc",
            SortDirection::Desc => "desc",
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(crate = "self::serde")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FilterExpression {
    #[prost(oneof = "Expression", tags = "1, 2")]
    pub expression: ::core::option::Option<Expression>,
}
#[derive(Serialize, Deserialize)]
#[serde(crate = "self::serde")]
#[derive(Clone, PartialEq, ::prost::Oneof)]
pub enum Expression {
    #[prost(message, tag = "1")]
    Simple(SimpleExpression),
    #[prost(message, tag = "2")]
    And(AndExpression),
}
#[derive(Serialize, Deserialize)]
#[serde(crate = "self::serde")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SimpleExpression {
    #[prost(string, tag = "1")]
    pub field: ::prost::alloc::string::String,
    #[prost(enumeration = "Operator", tag = "2")]
    pub operator: i32,
    #[prost(message, optional, tag = "3")]
    pub value: ::core::option::Option<::prost_wkt_types::Value>,
}
#[derive(Serialize, Deserialize)]
#[serde(crate = "self::serde")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Operator {
    Lt = 0,
    Lte = 1,
    Eq = 2,
    Gt = 3,
    Gte = 4,
    Contains = 5,
    MatchesAny = 6,
    MatchesAll = 7,
}
impl Operator {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Operator::Lt => "LT",
            Operator::Lte => "LTE",
            Operator::Eq => "EQ",
            Operator::Gt => "GT",
            Operator::Gte => "GTE",
            Operator::Contains => "Contains",
            Operator::MatchesAny => "MatchesAny",
            Operator::MatchesAll => "MatchesAll",
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(crate = "self::serde")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AndExpression {
    #[prost(message, repeated, tag = "1")]
    pub filter_expressions: ::prost::alloc::vec::Vec<FilterExpression>,
}
