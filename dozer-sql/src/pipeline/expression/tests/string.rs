use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::scalar::string::{
    evaluate_concat, evaluate_like, evaluate_trim, evaluate_ucase, validate_concat, validate_trim,
    TrimType,
};
use crate::pipeline::expression::tests::test_common::*;
use dozer_types::chrono::{DateTime, NaiveDate, TimeZone, Utc};
use dozer_types::types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition};
use proptest::prelude::*;

#[test]
fn test_string() {
    proptest!(
        ProptestConfig::with_cases(1000),
        move |(s_val in ".+", s_val1 in ".*", s_val2 in ".*", c_val: char)| {
            test_like(&s_val, c_val);
            test_ucase(&s_val, c_val);
            test_concat(&s_val1, &s_val2, c_val);
            test_trim(&s_val, c_val);
    });
}

fn test_like(s_val: &str, c_val: char) {
    let row = Record::new(vec![]);

    // Field::String
    let value = Box::new(Literal(Field::String(format!("Hello{}", s_val))));
    let pattern = Box::new(Literal(Field::String("Hello%".to_owned())));

    assert_eq!(
        evaluate_like(&Schema::default(), &value, &pattern, None, &row).unwrap(),
        Field::Boolean(true)
    );

    let value = Box::new(Literal(Field::String(format!("Hello, {}orld!", c_val))));
    let pattern = Box::new(Literal(Field::String("Hello, _orld!".to_owned())));

    assert_eq!(
        evaluate_like(&Schema::default(), &value, &pattern, None, &row).unwrap(),
        Field::Boolean(true)
    );

    let value = Box::new(Literal(Field::String(s_val.to_string())));
    let pattern = Box::new(Literal(Field::String("Hello%".to_owned())));

    assert_eq!(
        evaluate_like(&Schema::default(), &value, &pattern, None, &row).unwrap(),
        Field::Boolean(false)
    );

    let c_value = &s_val[0..0];
    let value = Box::new(Literal(Field::String(format!("Hello, {}!", c_value))));
    let pattern = Box::new(Literal(Field::String("Hello, _!".to_owned())));

    assert_eq!(
        evaluate_like(&Schema::default(), &value, &pattern, None, &row).unwrap(),
        Field::Boolean(false)
    );

    // todo: should find the way to generate escape character using proptest
    // let value = Box::new(Literal(Field::String(format!("Hello, {}%", c_val))));
    // let pattern = Box::new(Literal(Field::String("Hello, %".to_owned())));
    // let escape = Some(c_val);
    //
    // assert_eq!(
    //     evaluate_like(&Schema::default(), &value, &pattern, escape, &row).unwrap(),
    //     Field::Boolean(true)
    // );

    // Field::Text
    let value = Box::new(Literal(Field::Text(format!("Hello{}", s_val))));
    let pattern = Box::new(Literal(Field::Text("Hello%".to_owned())));

    assert_eq!(
        evaluate_like(&Schema::default(), &value, &pattern, None, &row).unwrap(),
        Field::Boolean(true)
    );

    let value = Box::new(Literal(Field::Text(format!("Hello, {}orld!", c_val))));
    let pattern = Box::new(Literal(Field::Text("Hello, _orld!".to_owned())));

    assert_eq!(
        evaluate_like(&Schema::default(), &value, &pattern, None, &row).unwrap(),
        Field::Boolean(true)
    );

    let value = Box::new(Literal(Field::Text(s_val.to_string())));
    let pattern = Box::new(Literal(Field::Text("Hello%".to_owned())));

    assert_eq!(
        evaluate_like(&Schema::default(), &value, &pattern, None, &row).unwrap(),
        Field::Boolean(false)
    );

    let c_value = &s_val[0..0];
    let value = Box::new(Literal(Field::Text(format!("Hello, {}!", c_value))));
    let pattern = Box::new(Literal(Field::Text("Hello, _!".to_owned())));

    assert_eq!(
        evaluate_like(&Schema::default(), &value, &pattern, None, &row).unwrap(),
        Field::Boolean(false)
    );

    // todo: should find the way to generate escape character using proptest
    // let value = Box::new(Literal(Field::Text(format!("Hello, {}%", c_val))));
    // let pattern = Box::new(Literal(Field::Text("Hello, %".to_owned())));
    // let escape = Some(c_val);
    //
    // assert_eq!(
    //     evaluate_like(&Schema::default(), &value, &pattern, escape, &row).unwrap(),
    //     Field::Boolean(true)
    // );
}

fn test_ucase(s_val: &str, c_val: char) {
    let row = Record::new(vec![]);

    // Field::String
    let value = Box::new(Literal(Field::String(s_val.to_string())));
    assert_eq!(
        evaluate_ucase(&Schema::default(), &value, &row).unwrap(),
        Field::String(s_val.to_uppercase())
    );

    let value = Box::new(Literal(Field::String(c_val.to_string())));
    assert_eq!(
        evaluate_ucase(&Schema::default(), &value, &row).unwrap(),
        Field::String(c_val.to_uppercase().to_string())
    );

    // Field::Text
    let value = Box::new(Literal(Field::Text(s_val.to_string())));
    assert_eq!(
        evaluate_ucase(&Schema::default(), &value, &row).unwrap(),
        Field::Text(s_val.to_uppercase())
    );

    let value = Box::new(Literal(Field::Text(c_val.to_string())));
    assert_eq!(
        evaluate_ucase(&Schema::default(), &value, &row).unwrap(),
        Field::Text(c_val.to_uppercase().to_string())
    );
}

fn test_concat(s_val1: &str, s_val2: &str, c_val: char) {
    let row = Record::new(vec![]);

    // Field::String
    let val1 = Literal(Field::String(s_val1.to_string()));
    let val2 = Literal(Field::String(s_val2.to_string()));

    if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
        assert_eq!(
            evaluate_concat(&Schema::default(), &[val1, val2], &row).unwrap(),
            Field::String(s_val1.to_string() + s_val2)
        );
    }

    let val1 = Literal(Field::String(s_val2.to_string()));
    let val2 = Literal(Field::String(s_val1.to_string()));

    if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
        assert_eq!(
            evaluate_concat(&Schema::default(), &[val1, val2], &row).unwrap(),
            Field::String(s_val2.to_string() + s_val1)
        );
    }

    let val1 = Literal(Field::String(s_val1.to_string()));
    let val2 = Literal(Field::String(c_val.to_string()));

    if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
        assert_eq!(
            evaluate_concat(&Schema::default(), &[val1, val2], &row).unwrap(),
            Field::String(s_val1.to_string() + c_val.to_string().as_str())
        );
    }

    let val1 = Literal(Field::String(c_val.to_string()));
    let val2 = Literal(Field::String(s_val1.to_string()));

    if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
        assert_eq!(
            evaluate_concat(&Schema::default(), &[val1, val2], &row).unwrap(),
            Field::String(c_val.to_string() + s_val1)
        );
    }

    // Field::Text
    let val1 = Literal(Field::Text(s_val1.to_string()));
    let val2 = Literal(Field::Text(s_val2.to_string()));

    if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
        assert_eq!(
            evaluate_concat(&Schema::default(), &[val1, val2], &row).unwrap(),
            Field::Text(s_val1.to_string() + s_val2)
        );
    }

    let val1 = Literal(Field::Text(s_val2.to_string()));
    let val2 = Literal(Field::Text(s_val1.to_string()));

    if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
        assert_eq!(
            evaluate_concat(&Schema::default(), &[val1, val2], &row).unwrap(),
            Field::Text(s_val2.to_string() + s_val1)
        );
    }

    let val1 = Literal(Field::Text(s_val1.to_string()));
    let val2 = Literal(Field::Text(c_val.to_string()));

    if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
        assert_eq!(
            evaluate_concat(&Schema::default(), &[val1, val2], &row).unwrap(),
            Field::Text(s_val1.to_string() + c_val.to_string().as_str())
        );
    }

    let val1 = Literal(Field::Text(c_val.to_string()));
    let val2 = Literal(Field::Text(s_val1.to_string()));

    if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
        assert_eq!(
            evaluate_concat(&Schema::default(), &[val1, val2], &row).unwrap(),
            Field::Text(c_val.to_string() + s_val1)
        );
    }
}

fn test_trim(s_val1: &str, c_val: char) {
    let row = Record::new(vec![]);

    // Field::String
    let value = Literal(Field::String(s_val1.to_string()));
    let what = ' ';

    if validate_trim(&value, &Schema::default()).is_ok() {
        assert_eq!(
            evaluate_trim(&Schema::default(), &value, &None, &None, &row).unwrap(),
            Field::String(s_val1.trim_matches(what).to_string())
        );
        assert_eq!(
            evaluate_trim(
                &Schema::default(),
                &value,
                &None,
                &Some(TrimType::Trailing),
                &row
            )
            .unwrap(),
            Field::String(s_val1.trim_end_matches(what).to_string())
        );
        assert_eq!(
            evaluate_trim(
                &Schema::default(),
                &value,
                &None,
                &Some(TrimType::Leading),
                &row
            )
            .unwrap(),
            Field::String(s_val1.trim_start_matches(what).to_string())
        );
        assert_eq!(
            evaluate_trim(
                &Schema::default(),
                &value,
                &None,
                &Some(TrimType::Both),
                &row
            )
            .unwrap(),
            Field::String(s_val1.trim_matches(what).to_string())
        );
    }

    let value = Literal(Field::String(s_val1.to_string()));
    let what = Some(Box::new(Literal(Field::String(c_val.to_string()))));

    if validate_trim(&value, &Schema::default()).is_ok() {
        assert_eq!(
            evaluate_trim(&Schema::default(), &value, &what, &None, &row).unwrap(),
            Field::String(s_val1.trim_matches(c_val).to_string())
        );
        assert_eq!(
            evaluate_trim(
                &Schema::default(),
                &value,
                &what,
                &Some(TrimType::Trailing),
                &row
            )
            .unwrap(),
            Field::String(s_val1.trim_end_matches(c_val).to_string())
        );
        assert_eq!(
            evaluate_trim(
                &Schema::default(),
                &value,
                &what,
                &Some(TrimType::Leading),
                &row
            )
            .unwrap(),
            Field::String(s_val1.trim_start_matches(c_val).to_string())
        );
        assert_eq!(
            evaluate_trim(
                &Schema::default(),
                &value,
                &what,
                &Some(TrimType::Both),
                &row
            )
            .unwrap(),
            Field::String(s_val1.trim_matches(c_val).to_string())
        );
    }
}

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
