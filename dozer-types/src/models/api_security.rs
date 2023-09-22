use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The security model option for the API
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
#[serde(deny_unknown_fields)]
pub enum ApiSecurity {
    /// Initialize with a JWT_SECRET
    Jwt(String),
}
