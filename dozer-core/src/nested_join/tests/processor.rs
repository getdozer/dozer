use crate::dag::node::ProcessorFactory;
use crate::nested_join::processor::{NestedJoinProcessor, NestedJoinProcessorFactory};
use crate::nested_join::tests::utils::{
    get_child_schema, get_expected_output_schema, get_input_schemas, get_parent_schema,
    get_processor, get_state_store, TestProcessorForwarder,
};
use crate::state::lmdb::LmdbStateStoreManager;
use crate::state::{StateStoreOptions, StateStoresManager};
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use std::collections::HashMap;
use std::fs;
use tempdir::TempDir;

#[test]
fn test_update_schema() {
    let mut processor = get_processor();

    let out_schema = processor
        .update_schema(
            NestedJoinProcessorFactory::OUTPUT_JOINED_PORT_HANDLE,
            &get_input_schemas(),
        )
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(out_schema, get_expected_output_schema());
}

#[test]
fn test_insert_child_without_parent() {
    let mut state_store = get_state_store();
    let mut processor = get_processor();
    let fw = TestProcessorForwarder::new();

    processor
        .update_schema(
            NestedJoinProcessorFactory::OUTPUT_JOINED_PORT_HANDLE,
            &get_input_schemas(),
        )
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    processor
        .process(
            NestedJoinProcessorFactory::INPUT_CHILD_PORT_HANDLE,
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![
                        Field::Int(100),
                        Field::Int(1),
                        Field::String("Kovan".to_string()),
                    ],
                ),
            },
            &fw,
            state_store.as_mut(),
        )
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(fw.res, vec![])
}
