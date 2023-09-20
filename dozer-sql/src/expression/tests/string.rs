use crate::expression::tests::test_common::*;
use dozer_types::chrono::{DateTime, NaiveDate, TimeZone, Utc};
use dozer_types::types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition};

#[test]
fn test_concat_string() {
    let f = run_fct(
        "SELECT CONCAT(fn, ln, fn) FROM USERS",
        Schema::default()
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
    let f = run_fct(
        "SELECT CONCAT(fn, ln, fn) FROM USERS",
        Schema::default()
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
    let f = run_fct(
        "SELECT CONCAT() FROM USERS",
        Schema::default()
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
    let f = run_fct(
        "SELECT CONCAT(fn, ln) FROM USERS",
        Schema::default()
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
fn test_ucase_string() {
    let f = run_fct(
        "SELECT UCASE(fn) FROM USERS",
        Schema::default()
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
    let f = run_fct(
        "SELECT UCASE(fn) FROM USERS",
        Schema::default()
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
    let f = run_fct(
        "SELECT LENGTH(fn) FROM USERS",
        Schema::default()
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
fn test_trim_string() {
    let f = run_fct(
        "SELECT TRIM(fn) FROM USERS",
        Schema::default()
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
    let f = run_fct(
        "SELECT TRIM(fn) FROM USERS",
        Schema::default()
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
    let f = run_fct(
        "SELECT TRIM(fn) FROM USERS",
        Schema::default()
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
    let f = run_fct(
        "SELECT TRIM('_' FROM fn) FROM USERS",
        Schema::default()
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
    let f = run_fct(
        "SELECT TRIM(BOTH '_' FROM fn) FROM USERS",
        Schema::default()
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
    let f = run_fct(
        "SELECT TRIM(LEADING '_' FROM fn) FROM USERS",
        Schema::default()
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
    let f = run_fct(
        "SELECT TRIM(TRAILING '_' FROM fn) FROM USERS",
        Schema::default()
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
fn test_like_value() {
    let f = run_fct(
        "SELECT first_name FROM users WHERE first_name LIKE 'J%'",
        Schema::default()
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
    let f = run_fct(
        "SELECT first_name FROM users WHERE first_name NOT LIKE 'A%'",
        Schema::default()
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
    let f = run_fct(
        "SELECT first_name FROM users WHERE first_name LIKE 'J$%'",
        Schema::default()
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

#[test]
fn test_to_char() {
    let f = run_fct(
        "SELECT TO_CHAR(ts, '%Y-%m-%d') FROM transactions",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("ts"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Timestamp(DateTime::from(
            Utc.timestamp_millis_opt(1672531200).unwrap(),
        ))],
    );
    assert_eq!(f, Field::String("1970-01-20".to_string()));

    let f = run_fct(
        "SELECT TO_CHAR(ts, '%H:%M') FROM transactions",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("ts"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Timestamp(DateTime::from(
            Utc.timestamp_millis_opt(1672531200).unwrap(),
        ))],
    );
    assert_eq!(f, Field::String("08:35".to_string()));

    let f = run_fct(
        "SELECT TO_CHAR(ts, '%H:%M') FROM transactions",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("ts"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Null],
    );
    assert_eq!(f, Field::Null);

    let f = run_fct(
        "SELECT TO_CHAR(ts, '%Y-%m-%d') FROM transactions",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("ts"),
                    FieldType::Date,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Date(NaiveDate::from_ymd_opt(2020, 1, 2).unwrap())],
    );
    assert_eq!(f, Field::String("2020-01-02".to_string()));

    let f = run_fct(
        "SELECT TO_CHAR(ts, '%H:%M') FROM transactions",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("ts"),
                    FieldType::Date,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Date(NaiveDate::from_ymd_opt(2020, 1, 2).unwrap())],
    );
    assert_eq!(f, Field::String("%H:%M".to_string()));
}
