use std::path::{Path, PathBuf};
use std::sync::Arc;

use dozer_cache::cache::{test_utils, LmdbRwCacheManager, RwCacheManager};
use dozer_types::models::api_endpoint::{ApiEndpoint, ApiIndex, ConflictResolution};
use dozer_types::types::{
    FieldDefinition, FieldType, IndexDefinition, Schema, SchemaIdentifier, SourceDefinition,
};

use crate::pipeline::LogSink;

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
    conflict_resolution: Option<ConflictResolution>,
) -> (Arc<dyn RwCacheManager>, LogSink) {
    let cache_manager = Arc::new(LmdbRwCacheManager::new(Default::default()).unwrap());

    let log_sink = LogSink::new(None, get_log_path(), "films").unwrap();

    (cache_manager, log_sink)
}
pub fn init_endpoint(conflict_resolution: Option<ConflictResolution>) -> ApiEndpoint {
    ApiEndpoint {
        name: "films".to_string(),
        path: "/films".to_string(),
        index: Some(ApiIndex {
            primary_key: vec!["film_id".to_string()],
        }),
        table_name: "films".to_string(),
        conflict_resolution,
        // sql: Some("SELECT film_name FROM film WHERE 1=1".to_string()),
    }
}

pub fn get_log_path() -> PathBuf {
    Path::new("./.dozer/pipeline/films").to_path_buf()
}
