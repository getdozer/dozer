use dozer_types::types::{FieldDefinition, FieldType, Schema, SourceDefinition};
use sqlparser::ast::Ident;

use crate::pipeline::expression::builder::{compare_name, get_field_index};

#[test]
fn test_compare_name() {
    assert!(compare_name("name".to_string(), "name".to_string()));
    assert!(compare_name("table.name".to_string(), "name".to_string()));
    assert!(compare_name("name".to_string(), "table.name".to_string()));
    assert!(compare_name(
        "table_a.name".to_string(),
        "table_a.name".to_string()
    ));

    assert!(!compare_name(
        "table_a.name".to_string(),
        "table_b.name".to_string()
    ));

    assert!(compare_name(
        "conn.table_a.name".to_string(),
        "name".to_string()
    ));

    assert!(compare_name(
        "conn.table_a.name".to_string(),
        "table_a.name".to_string()
    ));

    assert!(!compare_name(
        "conn.table_a.name".to_string(),
        "table_b.name".to_string()
    ));

    assert!(!compare_name(
        "conn.table_a.name".to_string(),
        "conn.table_b.name".to_string()
    ));

    assert!(compare_name(
        "conn.table_a.name".to_string(),
        "conn.table_a.name".to_string()
    ));

    assert!(!compare_name(
        "conn_a.table_a.name".to_string(),
        "conn_b.table_a.name".to_string()
    ));
}

#[test]
fn test_get_field_index() {
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                String::from("id"),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            true,
        )
        .field(
            FieldDefinition::new(
                String::from("name"),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                String::from("table.value"),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();

    let expected = 0;
    assert_eq!(
        get_field_index(&[Ident::new("id")], &schema).unwrap(),
        expected
    );

    let expected = 2;
    assert_eq!(
        get_field_index(&[Ident::new("value")], &schema).unwrap(),
        expected
    );

    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                String::from("id"),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            true,
        )
        .field(
            FieldDefinition::new(
                String::from("table_a.name"),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                String::from("table_b.name"),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();
    assert!(get_field_index(&[Ident::new("name")], &schema).is_err());
}
