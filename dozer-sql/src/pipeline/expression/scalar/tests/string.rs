use crate::pipeline::expression::scalar::tests::scalar_common::run_scalar_fct;
use dozer_types::types::{Field, FieldDefinition, FieldType, Schema};

#[test]
fn test_concat() {
    let f = run_scalar_fct(
        "SELECT CONCAT(fn, ln) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(String::from("fn"), FieldType::String, false),
                false,
            )
            .field(
                FieldDefinition::new(String::from("ln"), FieldType::String, false),
                false,
            )
            .clone(),
        vec![
            Field::String("John".to_string()),
            Field::String("Doe".to_string()),
        ],
    );
    assert_eq!(f, Field::String("JohnDoe".to_string()));
}

#[test]
#[should_panic]
fn test_concat_wrong_schema() {
    let f = run_scalar_fct(
        "SELECT CONCAT(fn, ln) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(String::from("fn"), FieldType::String, false),
                false,
            )
            .field(
                FieldDefinition::new(String::from("ln"), FieldType::Int, false),
                false,
            )
            .clone(),
        vec![Field::String("John".to_string()), Field::Int(0)],
    );
    assert_eq!(f, Field::String("JohnDoe".to_string()));
}

#[test]
fn test_ucase() {
    let f = run_scalar_fct(
        "SELECT UCASE(fn) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(String::from("fn"), FieldType::String, false),
                false,
            )
            .clone(),
        vec![Field::String("John".to_string())],
    );
    assert_eq!(f, Field::String("JOHN".to_string()));
}

#[test]
fn test_length() {
    let f = run_scalar_fct(
        "SELECT LENGTH(fn) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(String::from("fn"), FieldType::String, false),
                false,
            )
            .clone(),
        vec![Field::String("John".to_string())],
    );
    assert_eq!(f, Field::UInt(4));
}

#[test]
fn test_trim() {
    let f = run_scalar_fct(
        "SELECT TRIM_MATCH(fn) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(String::from("fn"), FieldType::String, false),
                false,
            )
            .clone(),
        vec![Field::String("   John   ".to_string())],
    );
    assert_eq!(f, Field::String("John".to_string()));
}

#[test]
fn test_trim_value() {
    let f = run_scalar_fct(
        "SELECT TRIM_MATCH(fn, ' ') FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(String::from("fn"), FieldType::String, false),
                false,
            )
            .clone(),
        vec![Field::String("   John   ".to_string())],
    );
    assert_eq!(f, Field::String("John".to_string()));
}
