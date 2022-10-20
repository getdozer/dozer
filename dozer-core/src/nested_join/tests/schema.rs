use crate::dag::dag::PortHandle;
use crate::nested_join::nested_join::{NestedJoinChildConfig, NestedJoinConfig};
use crate::nested_join::schema::{
    generate_nested_schema, generate_nested_schema_index, NestedJoinIndex, NestedJoinIndexParent,
};
use dozer_types::types::{FieldDefinition, FieldType, Schema};
use std::collections::HashMap;

fn get_company_schema() -> Schema {
    Schema::empty()
        .field(
            FieldDefinition::new("id".to_string(), FieldType::Int, false),
            true,
            true,
        )
        .field(
            FieldDefinition::new("company_name".to_string(), FieldType::Int, false),
            true,
            false,
        )
        .field(
            FieldDefinition::new(
                "offices".to_string(),
                FieldType::RecordArray(Schema::empty()),
                false,
            ),
            true,
            false,
        )
        .clone()
}

fn get_office_schema() -> Schema {
    Schema::empty()
        .field(
            FieldDefinition::new("id".to_string(), FieldType::Int, false),
            true,
            true,
        )
        .field(
            FieldDefinition::new("company_id".to_string(), FieldType::Int, false),
            true,
            false,
        )
        .field(
            FieldDefinition::new(
                "employees".to_string(),
                FieldType::RecordArray(Schema::empty()),
                false,
            ),
            true,
            false,
        )
        .clone()
}

fn get_employee_schema() -> Schema {
    Schema::empty()
        .field(
            FieldDefinition::new("id".to_string(), FieldType::Int, false),
            true,
            true,
        )
        .field(
            FieldDefinition::new("office_id".to_string(), FieldType::Int, false),
            true,
            false,
        )
        .field(
            FieldDefinition::new(
                "employee name".to_string(),
                FieldType::RecordArray(Schema::empty()),
                false,
            ),
            true,
            false,
        )
        .clone()
}

fn get_schema_nesting_rules() -> NestedJoinConfig {
    NestedJoinConfig {
        port: 1_u16,
        children: vec![NestedJoinChildConfig::new(
            2_u16,
            "offices".to_string(),
            vec!["id".to_string()],
            vec!["company_id".to_string()],
            vec![NestedJoinChildConfig::new(
                3_u16,
                "employees".to_string(),
                vec!["id".to_string()],
                vec!["office_id".to_string()],
                vec![],
            )],
        )],
    }
}

#[test]
fn test_generate_nested_schema_rules() {
    let company_schema = get_company_schema();
    let office_schema = get_office_schema();
    let employee_schema = get_employee_schema();

    let input_schemas: HashMap<PortHandle, Schema> = HashMap::from_iter(vec![
        (1_u16, company_schema),
        (2_u16, office_schema),
        (3_u16, employee_schema),
    ]);
    let nesting_rule = get_schema_nesting_rules();

    let r = generate_nested_schema_index(get_schema_nesting_rules(), &input_schemas)
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let expected_output = HashMap::from_iter(vec![
        (
            1_u16,
            NestedJoinIndex {
                id: 1_u16,
                parent: None,
                children: vec![2_u16],
            },
        ),
        (
            2_u16,
            NestedJoinIndex {
                id: 2_u16,
                parent: Some(NestedJoinIndexParent {
                    parent: 1_u16,
                    parent_array_index: 2,
                    parent_join_key_indexes: vec![0],
                    join_key_indexes: vec![1],
                }),
                children: vec![3_u16],
            },
        ),
        (
            3_u16,
            NestedJoinIndex {
                id: 3_u16,
                parent: Some(NestedJoinIndexParent {
                    parent: 2_u16,
                    parent_array_index: 2,
                    parent_join_key_indexes: vec![0],
                    join_key_indexes: vec![1],
                }),
                children: vec![],
            },
        ),
    ]);

    assert_eq!(r, expected_output);
}

#[test]
fn test_generate_nested_schema() {
    let company_schema = get_company_schema();
    let office_schema = get_office_schema();
    let employee_schema = get_employee_schema();
    let nesting_rule = get_schema_nesting_rules();

    let expected_schema = Schema::empty()
        .field(
            FieldDefinition::new("id".to_string(), FieldType::Int, false),
            true,
            true,
        )
        .field(
            FieldDefinition::new("company_name".to_string(), FieldType::Int, false),
            true,
            false,
        )
        .field(
            FieldDefinition::new(
                "offices".to_string(),
                FieldType::RecordArray(
                    Schema::empty()
                        .field(
                            FieldDefinition::new("id".to_string(), FieldType::Int, false),
                            true,
                            true,
                        )
                        .field(
                            FieldDefinition::new("company_id".to_string(), FieldType::Int, false),
                            true,
                            false,
                        )
                        .field(
                            FieldDefinition::new(
                                "employees".to_string(),
                                FieldType::RecordArray(
                                    Schema::empty()
                                        .field(
                                            FieldDefinition::new(
                                                "id".to_string(),
                                                FieldType::Int,
                                                false,
                                            ),
                                            true,
                                            true,
                                        )
                                        .field(
                                            FieldDefinition::new(
                                                "office_id".to_string(),
                                                FieldType::Int,
                                                false,
                                            ),
                                            true,
                                            false,
                                        )
                                        .field(
                                            FieldDefinition::new(
                                                "employee name".to_string(),
                                                FieldType::RecordArray(Schema::empty()),
                                                false,
                                            ),
                                            true,
                                            false,
                                        )
                                        .clone(),
                                ),
                                false,
                            ),
                            true,
                            false,
                        )
                        .clone(),
                ),
                false,
            ),
            true,
            false,
        )
        .clone();

    let input_schemas = HashMap::from_iter(vec![
        (1_u16, company_schema),
        (2_u16, office_schema),
        (3_u16, employee_schema),
    ]);

    let out_schema = generate_nested_schema(&input_schemas, get_schema_nesting_rules())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(out_schema, expected_schema);
}
