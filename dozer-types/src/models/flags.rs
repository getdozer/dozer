use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::equal_default;

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct Flags {
    /// dynamic grpc enabled; Default: true
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dynamic: Option<bool>,

    /// http1 + web support for grpc. This is required for browser clients.; Default: true
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc_web: Option<bool>,

    /// push events enabled.; Default: true
    #[serde(skip_serializing_if = "Option::is_none")]
    pub push_events: Option<bool>,

    /// require authentication to access grpc server reflection service if true.; Default: false
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authenticate_server_reflection: Option<bool>,

    /// probablistic optimizations reduce memory consumption at the expense of accuracy.
    #[serde(default, skip_serializing_if = "equal_default")]
    pub enable_probabilistic_optimizations: EnableProbabilisticOptimizations,

    /// app checkpoints can be used to resume execution of a query.; Default: false
    pub enable_app_checkpoints: Option<bool>,
}

pub fn default_dynamic() -> bool {
    true
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct EnableProbabilisticOptimizations {
    /// enable probabilistic optimizations in set operations (UNION, EXCEPT, INTERSECT); Default: false
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_sets: Option<bool>,

    /// enable probabilistic optimizations in JOIN operations; Default: false
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_joins: Option<bool>,

    /// enable probabilistic optimizations in aggregations (SUM, COUNT, MIN, etc.); Default: false
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_aggregations: Option<bool>,
}

pub fn default_push_events() -> bool {
    true
}

pub fn default_enable_app_checkpoints() -> bool {
    false
}
