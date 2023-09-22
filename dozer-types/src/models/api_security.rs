use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
#[doc = r"The security model option for the API"]
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub enum ApiSecurity {
    /// Initialize with a JWT_SECRET
    Jwt(String),
}
