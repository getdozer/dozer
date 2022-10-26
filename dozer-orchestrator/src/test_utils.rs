use crate::pipeline::CacheSink;
use dozer_cache::cache::LmdbCache;
use dozer_core::dag::mt_executor::DEFAULT_PORT_HANDLE;
use dozer_core::state::lmdb::LmdbStateStoreManager;
use dozer_types::core::node::PortHandle;
use dozer_types::core::state::{StateStore, StateStoreOptions, StateStoresManager};
use dozer_types::models::api_endpoint::{ApiEndpoint, ApiIndex};
use dozer_types::types::{FieldDefinition, FieldType, Schema, SchemaIdentifier};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use tempdir::TempDir;

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
        values: vec![0],
        primary_index: vec![0],
        secondary_indexes: vec![],
    }
}

pub fn init_sink(schema: &Schema) -> (Arc<LmdbCache>, CacheSink) {
    let cache = Arc::new(LmdbCache::new(true, None));

    let mut schema_map: HashMap<u64, bool> = HashMap::new();
    schema_map.insert(1, true);

    let mut input_schemas: HashMap<PortHandle, Schema> = HashMap::new();
    input_schemas.insert(DEFAULT_PORT_HANDLE, schema.clone());

    let sink = CacheSink::new(
        Arc::clone(&cache),
        init_endpoint(),
        input_schemas,
        schema_map,
    );
    (cache, sink)
}

pub fn init_state() -> Box<dyn StateStore> {
    let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to remove old dir"));
    }
    fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

    let sm = LmdbStateStoreManager::new(
        tmp_dir.path().to_str().unwrap().to_string(),
        1024 * 1024 * 1024 * 5,
        20_000,
    );
    let state = sm
        .init_state_store("1".to_string(), StateStoreOptions::default())
        .unwrap();
    state
}
pub fn init_endpoint() -> ApiEndpoint {
    ApiEndpoint {
        id: None,
        name: "films".to_string(),
        path: "/films".to_string(),
        enable_rest: false,
        enable_grpc: false,
        sql: "SELECT film_name FROM film WHERE 1=1".to_string(),
        index: ApiIndex {
            primary_key: vec!["film_id".to_string()],
        },
    }
}
