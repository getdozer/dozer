use crate::dag::dag::PortHandle;
use crate::dag::forwarder::ProcessorChannelForwarder;
use crate::dag::node::{Processor, ProcessorFactory};
use crate::nested_join::nested_join::{NestedJoinProcessor, NestedJoinProcessorFactory};
use crate::state::lmdb::LmdbStateStoreManager;
use crate::state::{StateStore, StateStoreOptions, StateStoresManager};
use dozer_types::types::{FieldDefinition, FieldType, Operation, Schema};
use std::collections::HashMap;
use std::fs;
use tempdir::TempDir;

pub fn get_parent_schema() -> Schema {
    Schema::empty()
        .field(
            FieldDefinition::new("id".to_string(), FieldType::Int, false),
            true,
            true,
        )
        .field(
            FieldDefinition::new("name".to_string(), FieldType::String, false),
            true,
            false,
        )
        .field(
            FieldDefinition::new(
                "addresses".to_string(),
                FieldType::RecordArray(Schema::empty()),
                false,
            ),
            true,
            false,
        )
        .clone()
}

pub fn get_child_schema() -> Schema {
    Schema::empty()
        .field(
            FieldDefinition::new("id".to_string(), FieldType::Int, false),
            true,
            true,
        )
        .field(
            FieldDefinition::new("customer_id".to_string(), FieldType::Int, false),
            true,
            false,
        )
        .field(
            FieldDefinition::new("addr_line".to_string(), FieldType::String, false),
            true,
            false,
        )
        .clone()
}

pub fn get_expected_output_schema() -> Schema {
    Schema::empty()
        .field(
            FieldDefinition::new("id".to_string(), FieldType::Int, false),
            true,
            true,
        )
        .field(
            FieldDefinition::new("name".to_string(), FieldType::String, false),
            true,
            false,
        )
        .field(
            FieldDefinition::new(
                "addresses".to_string(),
                FieldType::RecordArray(get_child_schema()),
                false,
            ),
            true,
            false,
        )
        .clone()
}

pub fn get_input_schemas() -> HashMap<PortHandle, Schema> {
    HashMap::from_iter(vec![
        (
            NestedJoinProcessorFactory::INPUT_PARENT_PORT_HANDLE,
            get_parent_schema(),
        ),
        (
            NestedJoinProcessorFactory::INPUT_CHILD_PORT_HANDLE,
            get_child_schema(),
        ),
    ])
}

pub fn get_processor() -> NestedJoinProcessor {
    NestedJoinProcessor::new(
        "addresses".to_string(),
        vec!["id".to_string()],
        vec!["customer_id".to_string()],
    )
}

pub fn get_state_store() -> Box<dyn StateStore> {
    let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to remove old dir"));
    }
    fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

    let mut state = LmdbStateStoreManager::new(
        tmp_dir.path().to_str().unwrap().to_string(),
        1024 * 1024 * 1024,
        10000,
    );

    state
        .init_state_store(
            "test".to_string(),
            StateStoreOptions {
                allow_duplicate_keys: true,
            },
        )
        .unwrap_or_else(|e| panic!("{}", e.to_string()))
}
