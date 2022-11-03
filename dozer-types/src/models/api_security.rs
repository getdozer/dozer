use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct APIConfiguration {
   pub port: u16,
   pub security: ApiSecurity
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum ApiSecurity {
    None,
    // Initialize with a JWT_SECRET
    Jwt(String),
}