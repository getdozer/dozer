use crate::dag::node::ProcessorFactory;
use crate::nested_join::nested_join::{NestedJoinProcessor, NestedJoinProcessorFactory};
use crate::nested_join::tests::utils::{
    get_child_schema, get_expected_output_schema, get_input_schemas, get_parent_schema,
    get_processor, get_state_store,
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
        .update_schema_op(
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

    processor
        .update_schema_op(
            NestedJoinProcessorFactory::OUTPUT_JOINED_PORT_HANDLE,
            &get_input_schemas(),
        )
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let r = processor
        .process_op(
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
            state_store.as_mut(),
        )
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(r, vec![])
}

#[test]
fn test_insert_parent_without_child() {
    let mut state_store = get_state_store();
    let mut processor = get_processor();

    processor
        .update_schema_op(
            NestedJoinProcessorFactory::OUTPUT_JOINED_PORT_HANDLE,
            &get_input_schemas(),
        )
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let r = processor
        .process_op(
            NestedJoinProcessorFactory::INPUT_PARENT_PORT_HANDLE,
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![
                        Field::Int(1),
                        Field::String("Matteo".to_string()),
                        Field::Null,
                    ],
                ),
            },
            state_store.as_mut(),
        )
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(r, vec![])
}

#[test]
fn test_insert_parent_with_children_with_pk() {
    let mut state_store = get_state_store();
    let mut processor = get_processor();

    processor
        .update_schema_op(
            NestedJoinProcessorFactory::OUTPUT_JOINED_PORT_HANDLE,
            &get_input_schemas(),
        )
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    processor
        .process_op(
            NestedJoinProcessorFactory::INPUT_CHILD_PORT_HANDLE,
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![
                        Field::Int(101),
                        Field::Int(1),
                        Field::String("Address 1".to_string()),
                    ],
                ),
            },
            state_store.as_mut(),
        )
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    processor
        .process_op(
            NestedJoinProcessorFactory::INPUT_CHILD_PORT_HANDLE,
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![
                        Field::Int(102),
                        Field::Int(1),
                        Field::String("Address 2".to_string()),
                    ],
                ),
            },
            state_store.as_mut(),
        )
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let r = processor
        .process_op(
            NestedJoinProcessorFactory::INPUT_PARENT_PORT_HANDLE,
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![
                        Field::Int(1),
                        Field::String("User 1".to_string()),
                        Field::Null,
                    ],
                ),
            },
            state_store.as_mut(),
        )
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let expected = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(1),
                Field::String("User 1".to_string()),
                Field::RecordArray(vec![
                    Operation::Insert {
                        new: Record::new(
                            None,
                            vec![
                                Field::Int(101),
                                Field::Int(1),
                                Field::String("Address 1".to_string()),
                            ],
                        ),
                    },
                    Operation::Insert {
                        new: Record::new(
                            None,
                            vec![
                                Field::Int(102),
                                Field::Int(1),
                                Field::String("Address 2".to_string()),
                            ],
                        ),
                    },
                ]),
            ],
        ),
    };

    assert_eq!(r[0], expected)
}
