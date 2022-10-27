use crate::pipeline::CacheSink;
use dozer_cache::cache::LmdbCache;
use dozer_core::dag::mt_executor::DEFAULT_PORT_HANDLE;
use dozer_types::core::node::PortHandle;
use dozer_types::models::api_endpoint::{ApiEndpoint, ApiIndex};
use dozer_types::types::{FieldDefinition, FieldType, Schema, SchemaIdentifier};
use rocksdb::{Options, DB};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use dozer_types::chk;
use dozer_types::test_helper::get_temp_dir;
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
    let cache = Arc::new(LmdbCache::new(true));

    let mut schema_map: HashMap<u64, bool> = HashMap::new();
    schema_map.insert(1, true);

    let mut input_schemas: HashMap<PortHandle, Schema> = HashMap::new();
    input_schemas.insert(DEFAULT_PORT_HANDLE, schema.clone());

    let sink = CacheSink::new(
        Arc::clone(&cache),
        init_endpoint(),
        input_schemas,
        schema_map,
        None,
    );
    (cache, sink)
}

pub fn init_state() -> DB {
    let mut opts = Options::default();
    opts.set_allow_mmap_writes(true);
    opts.optimize_for_point_lookup(1024 * 1024 * 1024);
    opts.set_bytes_per_sync(1024 * 1024 * 10);
    opts.set_manual_wal_flush(true);
    opts.create_if_missing(true);

    chk!(DB::open(&opts, get_temp_dir()))
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
