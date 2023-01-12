use serde::{Deserialize, Serialize};
#[doc = r"The security model option for the API"]
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Oneof, Hash)]
pub enum ApiSecurity {
    /// Initialize with a JWT_SECRET
    #[prost(string, tag = "1")]
    Jwt(String),
}
