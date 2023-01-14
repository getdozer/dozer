use crate::pipeline::CacheSink;
use dozer_cache::cache::{CacheOptions, LmdbCache};
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_types::models::api_endpoint::{ApiEndpoint, ApiIndex};
use dozer_types::types::{FieldDefinition, FieldType, IndexDefinition, Schema, SchemaIdentifier};
use std::collections::HashMap;
use std::sync::Arc;

pub fn get_schema() -> Schema {
    Schema {
        identifier: Option::from(SchemaIdentifier { id: 1, version: 1 }),
        fields: vec![
            FieldDefinition {
                name: "film_id".to_string(),
                typ: FieldType::Int,
                nullable: false,
            },
            FieldDefinition {
                name: "film_name".to_string(),
                typ: FieldType::String,
                nullable: false,
            },
        ],
        primary_index: vec![0],
    }
}

pub fn init_sink(
    schema: &Schema,
    secondary_indexes: Vec<IndexDefinition>,
) -> (Arc<LmdbCache>, CacheSink) {
    let cache = Arc::new(LmdbCache::new(CacheOptions::default()).unwrap());

    let mut input_schemas = HashMap::new();
    input_schemas.insert(DEFAULT_PORT_HANDLE, (schema.clone(), secondary_indexes));

    let sink = CacheSink::new(
        Arc::clone(&cache),
        init_endpoint(),
        input_schemas,
        None,
        None,
    );
    (cache, sink)
}
pub fn init_endpoint() -> ApiEndpoint {
    ApiEndpoint {
        id: None,
        name: "films".to_string(),
        path: "/films".to_string(),
        sql: "SELECT film_name FROM film WHERE 1=1".to_string(),
        index: Some(ApiIndex {
            primary_key: vec!["film_id".to_string()],
        }),
        app_id: None,
    }
}
