use dozer_cache::cache::expression::FilterExpression;
use dozer_types::serde;
use serde::{Deserialize, Serialize};
pub mod api;
mod authorizer;
pub use authorizer::Authorizer;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(crate = "self::serde")]
pub struct Claims {
    pub aud: String,
    pub sub: String,
    pub exp: usize,
    pub access: Access,
}
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(crate = "self::serde")]
// Access gets resolved in cache query, get and list functions
pub enum Access {
    /// Access to all indexes
    All,
    /// Specific permissions to each of the indexes
    Custom(Vec<AccessFilter>),
}
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(crate = "self::serde")]
/// This filter gets dynamically added to the query.
pub struct AccessFilter {
    /// Name of the index
    indexes: Vec<String>,

    /// FilterExpression to evaluate access
    filter: Option<FilterExpression>,

    /// Fields to be restricted
    fields: Vec<String>,
}
