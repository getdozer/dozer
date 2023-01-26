use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::scalar::{
    string::evaluate_like, tests::scalar_common::run_scalar_fct,
};
use dozer_types::types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition};

#[test]
fn test_concat() {
    let f = run_scalar_fct(
        "SELECT CONCAT(fn, ln, fn) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("ln"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![
            Field::String("John".to_string()),
            Field::String("Doe".to_string()),
        ],
    );
    assert_eq!(f, Field::String("JohnDoeJohn".to_string()));
}

#[test]
fn test_concat_text() {
    let f = run_scalar_fct(
        "SELECT CONCAT(fn, ln, fn) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::Text,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("ln"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![
            Field::Text("John".to_string()),
            Field::String("Doe".to_string()),
        ],
    );
    assert_eq!(f, Field::Text("JohnDoeJohn".to_string()));
}

#[test]
fn test_concat_text_empty() {
    let f = run_scalar_fct(
        "SELECT CONCAT() FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("ln"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![
            Field::String("John".to_string()),
            Field::String("Doe".to_string()),
        ],
    );
    assert_eq!(f, Field::String("".to_string()));
}

#[test]
#[should_panic]
fn test_concat_wrong_schema() {
    let f = run_scalar_fct(
        "SELECT CONCAT(fn, ln) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("ln"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
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
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::String("John".to_string())],
    );
    assert_eq!(f, Field::String("JOHN".to_string()));
}

#[test]
fn test_ucase_text() {
    let f = run_scalar_fct(
        "SELECT UCASE(fn) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::Text,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Text("John".to_string())],
    );
    assert_eq!(f, Field::Text("JOHN".to_string()));
}

#[test]
fn test_length() {
    let f = run_scalar_fct(
        "SELECT LENGTH(fn) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
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
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::String("   John   ".to_string())],
    );
    assert_eq!(f, Field::String("John".to_string()));
}

#[test]
fn test_trim_null() {
    let f = run_scalar_fct(
        "SELECT TRIM(fn) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Null],
    );
    assert_eq!(f, Field::String("".to_string()));
}

#[test]
fn test_trim_text() {
    let f = run_scalar_fct(
        "SELECT TRIM(fn) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::Text,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Text("   John   ".to_string())],
    );
    assert_eq!(f, Field::Text("John".to_string()));
}

#[test]
fn test_trim_value() {
    let f = run_scalar_fct(
        "SELECT TRIM('_' FROM fn) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
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
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
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
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
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
                FieldDefinition::new(
                    String::from("fn"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::String("___John___".to_string())],
    );
    assert_eq!(f, Field::String("___John".to_string()));
}

#[test]
fn test_like() {
    let row = Record::new(None, vec![], None);

    let value = Box::new(Literal(Field::String("Hello, World!".to_owned())));
    let pattern = Box::new(Literal(Field::String("Hello%".to_owned())));

    assert_eq!(
        evaluate_like(&Schema::empty(), &value, &pattern, None, &row).unwrap(),
        Field::Boolean(true)
    );

    let value = Box::new(Literal(Field::String("Hello, World!".to_owned())));
    let pattern = Box::new(Literal(Field::String("Hello, _orld!".to_owned())));

    assert_eq!(
        evaluate_like(&Schema::empty(), &value, &pattern, None, &row).unwrap(),
        Field::Boolean(true)
    );

    let value = Box::new(Literal(Field::String("Bye, World!".to_owned())));
    let pattern = Box::new(Literal(Field::String("Hello%".to_owned())));

    assert_eq!(
        evaluate_like(&Schema::empty(), &value, &pattern, None, &row).unwrap(),
        Field::Boolean(false)
    );

    let value = Box::new(Literal(Field::String("Hello, World!".to_owned())));
    let pattern = Box::new(Literal(Field::String("Hello, _!".to_owned())));

    assert_eq!(
        evaluate_like(&Schema::empty(), &value, &pattern, None, &row).unwrap(),
        Field::Boolean(false)
    );

    let value = Box::new(Literal(Field::String("Hello, $%".to_owned())));
    let pattern = Box::new(Literal(Field::String("Hello, %".to_owned())));
    let escape = Some('$');

    assert_eq!(
        evaluate_like(&Schema::empty(), &value, &pattern, escape, &row).unwrap(),
        Field::Boolean(true)
    );
}

#[test]
fn test_like_value() {
    let f = run_scalar_fct(
        "SELECT first_name FROM users WHERE first_name LIKE 'J%'",
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
            .clone(),
        vec![Field::String("John".to_string())],
    );
    assert_eq!(f, Field::String("John".to_string()));
}

#[test]
fn test_not_like_value() {
    let f = run_scalar_fct(
        "SELECT first_name FROM users WHERE first_name NOT LIKE 'A%'",
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
            .clone(),
        vec![Field::String("John".to_string())],
    );
    assert_eq!(f, Field::String("John".to_string()));
}

#[test]
fn test_like_escape() {
    let f = run_scalar_fct(
        "SELECT first_name FROM users WHERE first_name LIKE 'J$%'",
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
            .clone(),
        vec![Field::String("J%".to_string())],
    );
    assert_eq!(f, Field::String("J%".to_string()));
}
