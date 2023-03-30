use crate::pipeline::expression::tests::test_common::run_fct;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition};

#[test]
fn test_validate_coalesce() {}

#[test]
fn test_coalesce_logic() {
    let f = run_fct(
        "SELECT COALESCE(field, 2) FROM users",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Null],
    );
    assert_eq!(f, Field::Int(2));

    let f = run_fct(
        "SELECT COALESCE(field, CAST(2 AS FLOAT)) FROM users",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Float,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Null],
    );
    assert_eq!(f, Field::Float(OrderedFloat(2.0)));

    let f = run_fct(
        "SELECT COALESCE(field, 'X') FROM users",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Null],
    );
    assert_eq!(f, Field::String("X".to_string()));

    let f = run_fct(
        "SELECT COALESCE(field, 'X') FROM users",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Null],
    );
    assert_eq!(f, Field::String("X".to_string()));
}

#[test]
fn test_coalesce_logic_null() {
    let f = run_fct(
        "SELECT COALESCE(field) FROM users",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Null],
    );
    assert_eq!(f, Field::Null);
}
