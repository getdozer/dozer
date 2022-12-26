use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Oneof, Hash)]
pub enum ApiSecurity {
    // Initialize with a JWT_SECRET
    #[prost(string, tag = "1")]
    Jwt(String),
}
