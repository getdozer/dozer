use dozer_types::chrono::{DateTime, NaiveDate};
use dozer_types::types::DATE_FORMAT;
use dozer_types::types::{Field, Schema};
use dozer_types::types::{FieldDefinition, FieldType, SourceDefinition};

use crate::expression::tests::test_common::run_fct;

#[test]
fn test_comparison_logical_int() {
    let record = vec![Field::Int(124)];
    let schema = Schema::default()
        .field(
            FieldDefinition::new(
                String::from("id"),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();

    let f = run_fct(
        "SELECT id FROM users WHERE id = '124'",
        schema.clone(),
        record.clone(),
    );
    assert_eq!(f, Field::Int(124));

    let f = run_fct(
        "SELECT id FROM users WHERE id <= '124'",
        schema.clone(),
        record.clone(),
    );
    assert_eq!(f, Field::Int(124));

    let f = run_fct(
        "SELECT id FROM users WHERE id >= '124'",
        schema.clone(),
        record.clone(),
    );
    assert_eq!(f, Field::Int(124));

    let f = run_fct(
        "SELECT id = '124' FROM users",
        schema.clone(),
        record.clone(),
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct(
        "SELECT id < '124' FROM users",
        schema.clone(),
        record.clone(),
    );
    assert_eq!(f, Field::Boolean(false));

    let f = run_fct(
        "SELECT id > '124' FROM users",
        schema.clone(),
        record.clone(),
    );
    assert_eq!(f, Field::Boolean(false));

    let f = run_fct(
        "SELECT id <= '124' FROM users",
        schema.clone(),
        record.clone(),
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct("SELECT id >= '124' FROM users", schema, record);
    assert_eq!(f, Field::Boolean(true));
}

#[test]
fn test_comparison_logical_timestamp() {
    let f = run_fct(
        "SELECT time = '2020-01-01T00:00:00Z' FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("time"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Timestamp(
            DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap(),
        )],
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct(
        "SELECT time < '2020-01-01T00:00:01Z' FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("time"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Timestamp(
            DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap(),
        )],
    );
    assert_eq!(f, Field::Boolean(true));
}

#[test]
fn test_comparison_logical_date() {
    let f = run_fct(
        "SELECT date = '2020-01-01' FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("date"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Date(
            NaiveDate::parse_from_str("2020-01-01", DATE_FORMAT).unwrap(),
        )],
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct(
        "SELECT date != '2020-01-01' FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("date"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Date(
            NaiveDate::parse_from_str("2020-01-01", DATE_FORMAT).unwrap(),
        )],
    );
    assert_eq!(f, Field::Boolean(false));

    let f = run_fct(
        "SELECT date > '2020-01-01' FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("date"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Date(
            NaiveDate::parse_from_str("2020-01-02", DATE_FORMAT).unwrap(),
        )],
    );
    assert_eq!(f, Field::Boolean(true));
}
