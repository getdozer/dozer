use crate::pipeline::CacheSink;
use dozer_cache::cache::{LmdbRwCache, RwCache};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::models::api_endpoint::{ApiEndpoint, ApiIndex};
use dozer_types::types::{
    FieldDefinition, FieldType, IndexDefinition, Schema, SchemaIdentifier, SourceDefinition,
};
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
                source: SourceDefinition::Dynamic,
            },
            FieldDefinition {
                name: "film_name".to_string(),
                typ: FieldType::String,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
        ],
        primary_index: vec![0],
    }
}

pub fn init_sink(
    schema: &Schema,
    secondary_indexes: Vec<IndexDefinition>,
) -> (Arc<dyn RwCache>, CacheSink) {
    let cache: Arc<dyn RwCache> =
        Arc::new(LmdbRwCache::new(Default::default(), Default::default()).unwrap());

    let mut input_schemas = HashMap::new();
    input_schemas.insert(DEFAULT_PORT_HANDLE, (schema.clone(), secondary_indexes));

    let sink = CacheSink::new(cache.clone(), init_endpoint(), input_schemas, None, None);
    (cache, sink)
}
pub fn init_endpoint() -> ApiEndpoint {
    ApiEndpoint {
        name: "films".to_string(),
        path: "/films".to_string(),
        index: Some(ApiIndex {
            primary_key: vec!["film_id".to_string()],
        }),
        table_name: "films".to_string(),
        // sql: Some("SELECT film_name FROM film WHERE 1=1".to_string()),
    }
}
