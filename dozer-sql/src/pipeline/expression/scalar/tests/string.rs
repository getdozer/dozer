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
        "SELECT TRIM(fn) FROM USERS",
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
        "SELECT TRIM('_' FROM fn) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(String::from("fn"), FieldType::String, false),
                false,
            )
            .clone(),
        vec![Field::String("___John___".to_string())],
    );
    assert_eq!(f, Field::String("John".to_string()));
}

#[test]
fn test_btrim_value() {
    let f = run_scalar_fct(
        "SELECT TRIM(BOTH '_' FROM fn) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(String::from("fn"), FieldType::String, false),
                false,
            )
            .clone(),
        vec![Field::String("___John___".to_string())],
    );
    assert_eq!(f, Field::String("John".to_string()));
}

#[test]
fn test_ltrim_value() {
    let f = run_scalar_fct(
        "SELECT TRIM(LEADING '_' FROM fn) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(String::from("fn"), FieldType::String, false),
                false,
            )
            .clone(),
        vec![Field::String("___John___".to_string())],
    );
    assert_eq!(f, Field::String("John___".to_string()));
}

#[test]
fn test_ttrim_value() {
    let f = run_scalar_fct(
        "SELECT TRIM(TRAILING '_' FROM fn) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(String::from("fn"), FieldType::String, false),
                false,
            )
            .clone(),
        vec![Field::String("___John___".to_string())],
    );
    assert_eq!(f, Field::String("___John".to_string()));
}
