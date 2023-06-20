use crate::pipeline::expression::tests::test_common::run_fct;
use dozer_types::types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition};

#[test]
fn test_case() {
    let f = run_fct(
        "SELECT \
                CASE \
                    WHEN age > 11 THEN 'The age is greater than 11' \
                    WHEN age = 11 THEN 'The age is 11' \
                    ELSE 'The age is under 11' \
                END AS age_text \
            FROM users",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("first_name"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("age"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::String("chloe".to_string()), Field::Int(11)],
    );
    assert_eq!(f, Field::String("The age is 11".to_string()));
}

#[test]
fn test_case_else() {
    let f = run_fct(
        "SELECT \
                CASE \
                    WHEN age > 11 THEN 'The age is greater than 11' \
                    WHEN age = 11 THEN 'The age is 11' \
                    ELSE 'The age is under 11' \
                END AS age_text \
            FROM users",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("first_name"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("age"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::String("chloe".to_string()), Field::Int(0)],
    );
    assert_eq!(f, Field::String("The age is under 11".to_string()));
}

#[test]
fn test_case_no_else() {
    let f = run_fct(
        "SELECT \
                CASE \
                    WHEN age > 11 THEN 'The age is greater than 11' \
                    WHEN age = 11 THEN 'The age is 11' \
                END AS age_text \
            FROM users",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("first_name"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("age"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::String("chloe".to_string()), Field::Int(9)],
    );
    assert_eq!(f, Field::Null);
}
