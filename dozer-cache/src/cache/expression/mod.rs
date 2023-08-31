use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::serde_json;
use dozer_types::serde_json::Value;
mod query_helper;
mod query_serde;
use actix_web::{dev::Payload, Error, FromRequest, HttpMessage, HttpRequest};
use futures::future::err;
use futures::future::LocalBoxFuture;
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

pub fn default_limit_for_query() -> usize {
    50
}

impl QueryExpression {
    pub fn with_default_limit() -> Self {
        Self {
            filter: None,
            order_by: Default::default(),
            limit: Some(default_limit_for_query()),
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
        Self::with_default_limit()
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

impl FromRequest for QueryExpression {
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let req = req.clone();
        let mut pl = _payload.take();
        let content_type = req.content_type();
        if !content_type.is_empty() && content_type != "application/json" {
            return Box::pin(err(actix_web::error::UrlencodedError::ContentType.into()));
        }
        //execute query
        Box::pin(async move {
            let req_body = String::from_request(&req, &mut pl).await?;
            if req_body.is_empty() {
                return Ok(QueryExpression::with_no_limit());
            }
            let deserialized = serde_json::from_str::<QueryExpression>(&req_body);
            match deserialized {
                Ok(x) => Ok(x),
                Err(x) => Err(actix_web::error::JsonPayloadError::Deserialize(x).into()),
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FilterExpression {
    // a = 1, a containts "s", a > 4
    Simple(String, Operator, Value),
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
