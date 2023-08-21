use std::collections::HashMap;

use dozer_cache::AccessFilter;
use dozer_types::serde;
use serde::{Deserialize, Serialize};
pub mod api;
pub mod authorizer;
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
    /// (endpoint_name, AccessFilter) Specific permissions to each of the indexes
    Custom(HashMap<String, AccessFilter>),
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dozer_cache::AccessFilter;
    use dozer_types::serde_json::json;

    use super::Access;

    #[test]
    fn serialize_access() {
        let mut access_map = HashMap::new();
        access_map.insert(
            "films".to_string(),
            AccessFilter {
                filter: None,
                fields: vec![],
            },
        );
        let access = Access::Custom(access_map);
        let res = dozer_types::serde_json::to_string(&access);

        assert!(res.is_ok());

        let de_access = dozer_types::serde_json::from_value::<Access>(
            json!({"Custom":{"films":{"filter":null,"fields":[]}}}),
        );

        assert_eq!(de_access.unwrap(), access);

        let de_access = dozer_types::serde_json::from_value::<Access>(json!("All"));
        assert_eq!(de_access.unwrap(), Access::All);
    }
}
