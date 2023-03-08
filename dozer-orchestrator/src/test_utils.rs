use std::sync::Arc;

use crate::pipeline::CacheSink;
use dozer_cache::cache::{CacheManager, LmdbCacheManager};
use dozer_types::models::api_endpoint::{ApiEndpoint, ApiIndex};
use dozer_types::types::{
    FieldDefinition, FieldType, IndexDefinition, Schema, SchemaIdentifier, SourceDefinition,
};

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
    schema: Schema,
    secondary_indexes: Vec<IndexDefinition>,
) -> (Arc<dyn CacheManager>, CacheSink) {
    let cache_manager = Arc::new(LmdbCacheManager::new(Default::default()).unwrap());
    let cache = CacheSink::new(
        cache_manager.clone(),
        init_endpoint(),
        schema,
        secondary_indexes,
        None,
        None,
    )
    .unwrap();
    (cache_manager, cache)
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
