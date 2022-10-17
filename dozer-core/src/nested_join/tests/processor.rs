use crate::dag::node::ProcessorFactory;
use crate::nested_join::processor::{NestedJoinProcessor, NestedJoinProcessorFactory};
use crate::nested_join::tests::utils::{
    get_child_schema, get_expected_output_schema, get_input_schemas, get_parent_schema,
    get_processor,
};
use crate::state::lmdb::LmdbStateStoreManager;
use crate::state::{StateStoreOptions, StateStoresManager};
use dozer_types::types::{FieldDefinition, FieldType, Schema};
use std::collections::HashMap;
use std::fs;
use tempdir::TempDir;

#[test]
fn test_children_lookup() {
    let mut processor = get_processor();

    let out_schema = processor
        .update_schema(
            NestedJoinProcessorFactory::OUTPUT_JOINED_PORT_HANDLE,
            &get_input_schemas(),
        )
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(out_schema, get_expected_output_schema());
}
