use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, JsonSchema, Deserialize, PartialEq, Eq, Clone, Default)]
pub struct Flags {
    /// dynamic grpc enabled; Default: true

    #[serde(default = "default_true")]
    pub dynamic: bool,
    /// http1 + web support for grpc. This is required for browser clients.; Default: true

    #[serde(default = "default_true")]
    pub grpc_web: bool,

    /// push events enabled.; Default: true

    #[serde(default = "default_push_events")]
    pub push_events: bool,

    /// require authentication to access grpc server reflection service if true.; Default: false

    #[serde(default = "default_false")]
    pub authenticate_server_reflection: bool,

    /// probablistic optimizations reduce memory consumption at the expense of accuracy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_probabilistic_optimizations: Option<EnableProbabilisticOptimizations>,
}

#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, PartialEq, Eq, Clone)]
pub struct EnableProbabilisticOptimizations {
    /// enable probabilistic optimizations in set operations (UNION, EXCEPT, INTERSECT); Default: false

    #[serde(default = "default_false")]
    pub in_sets: bool,

    /// enable probabilistic optimizations in JOIN operations; Default: false

    #[serde(default = "default_false")]
    pub in_joins: bool,

    /// enable probabilistic optimizations in aggregations (SUM, COUNT, MIN, etc.); Default: false

    #[serde(default = "default_false")]
    pub in_aggregations: bool,
}

pub fn default_push_events() -> bool {
    true
}

fn default_true() -> bool {
    true
}
fn default_false() -> bool {
    false
}
