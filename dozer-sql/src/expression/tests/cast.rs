use crate::expression::tests::test_common::*;
use dozer_types::types::SourceDefinition;
use dozer_types::{
    chrono::{DateTime, NaiveDate, TimeZone, Utc},
    ordered_float::OrderedFloat,
    rust_decimal::Decimal,
    types::{Field, FieldDefinition, FieldType, Schema},
};

#[test]
fn test_uint() {
    let f = run_fct(
        "SELECT CAST(field AS UINT) FROM users",
        Schema::default()
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
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::UInt(42));

    let f = run_fct(
        "SELECT CAST(field AS UINT) FROM users",
        Schema::default()
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
        vec![Field::String("42".to_string())],
    );
    assert_eq!(f, Field::UInt(42));

    let f = run_fct(
        "SELECT CAST(field AS UINT) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::UInt,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::UInt(42)],
    );
    assert_eq!(f, Field::UInt(42));
}

#[test]
fn test_u128() {
    let f = run_fct(
        "SELECT CAST(field AS U128) FROM users",
        Schema::default()
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
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::U128(42));

    let f = run_fct(
        "SELECT CAST(field AS U128) FROM users",
        Schema::default()
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
        vec![Field::String("42".to_string())],
    );
    assert_eq!(f, Field::U128(42));

    let f = run_fct(
        "SELECT CAST(field AS U128) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::UInt,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::UInt(42)],
    );
    assert_eq!(f, Field::U128(42));
}

#[test]
fn test_int() {
    let f = run_fct(
        "SELECT CAST(field AS INT) FROM users",
        Schema::default()
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
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::Int(42));

    let f = run_fct(
        "SELECT CAST(field AS INT) FROM users",
        Schema::default()
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
        vec![Field::String("42".to_string())],
    );
    assert_eq!(f, Field::Int(42));

    let f = run_fct(
        "SELECT CAST(field AS INT) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::UInt,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::UInt(42)],
    );
    assert_eq!(f, Field::Int(42));
}

#[test]
fn test_i128() {
    let f = run_fct(
        "SELECT CAST(field AS I128) FROM users",
        Schema::default()
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
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::I128(42));

    let f = run_fct(
        "SELECT CAST(field AS I128) FROM users",
        Schema::default()
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
        vec![Field::String("42".to_string())],
    );
    assert_eq!(f, Field::I128(42));

    let f = run_fct(
        "SELECT CAST(field AS I128) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::UInt,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::UInt(42)],
    );
    assert_eq!(f, Field::I128(42));
}

#[test]
fn test_float() {
    let f = run_fct(
        "SELECT CAST(field AS FLOAT) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Decimal,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Decimal(Decimal::new(42, 1))],
    );
    assert_eq!(
        f,
        Field::Float(dozer_types::ordered_float::OrderedFloat(4.2))
    );

    let f = run_fct(
        "SELECT CAST(field AS FLOAT) FROM users",
        Schema::default()
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
        vec![Field::Float(OrderedFloat(4.2))],
    );
    assert_eq!(f, Field::Float(OrderedFloat(4.2)));

    let f = run_fct(
        "SELECT CAST(field AS FLOAT) FROM users",
        Schema::default()
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
        vec![Field::Int(4)],
    );
    assert_eq!(f, Field::Float(OrderedFloat(4.0)));

    let f = run_fct(
        "SELECT CAST(field AS FLOAT) FROM users",
        Schema::default()
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
        vec![Field::String("4.2".to_string())],
    );
    assert_eq!(f, Field::Float(OrderedFloat(4.2)));

    let f = run_fct(
        "SELECT CAST(field AS FLOAT) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::UInt,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::UInt(4)],
    );
    assert_eq!(f, Field::Float(OrderedFloat(4.0)));
}

#[test]
fn test_boolean() {
    let f = run_fct(
        "SELECT CAST(field AS BOOLEAN) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Boolean,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Boolean(true)],
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct(
        "SELECT CAST(field AS BOOLEAN) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Decimal,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Decimal(Decimal::new(0, 0))],
    );
    assert_eq!(f, Field::Boolean(false));

    let f = run_fct(
        "SELECT CAST(field AS BOOLEAN) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Decimal,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Decimal(Decimal::new(1, 0))],
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct(
        "SELECT CAST(field AS BOOLEAN) FROM users",
        Schema::default()
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
        vec![Field::Float(OrderedFloat(1.0))],
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct(
        "SELECT CAST(field AS BOOLEAN) FROM users",
        Schema::default()
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
        vec![Field::Int(0)],
    );
    assert_eq!(f, Field::Boolean(false));

    let f = run_fct(
        "SELECT CAST(field AS BOOLEAN) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::UInt,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::UInt(1)],
    );
    assert_eq!(f, Field::Boolean(true));
}

#[test]
fn test_string() {
    // let f = run_scalar_fct(
    //     "SELECT CAST(field AS STRING) FROM users",
    //     Schema::default()
    //         .field(
    //             FieldDefinition::new(String::from("field"), FieldType::Binary, false),
    //             false,
    //         )
    //         .clone(),
    //     vec![Field::Binary(vec![])],
    // );
    // assert_eq!(f, Field::String("".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS STRING) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Boolean,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Boolean(true)],
    );
    assert_eq!(f, Field::String("TRUE".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS STRING) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Date,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Date(NaiveDate::from_ymd_opt(2022, 1, 1).unwrap())],
    );
    assert_eq!(f, Field::String("2022-01-01".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS STRING) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Decimal,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Decimal(Decimal::new(42, 1))],
    );
    assert_eq!(f, Field::String("4.2".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS STRING) FROM users",
        Schema::default()
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
        vec![Field::Float(OrderedFloat(4.2))],
    );
    assert_eq!(f, Field::String("4.2".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS STRING) FROM users",
        Schema::default()
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
        vec![Field::Int(-42)],
    );
    assert_eq!(f, Field::String("-42".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS STRING) FROM users",
        Schema::default()
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
        vec![Field::String("Hello".to_string())],
    );
    assert_eq!(f, Field::String("Hello".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS STRING) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Text,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Text("Hello".to_string())],
    );
    assert_eq!(f, Field::String("Hello".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS STRING) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Timestamp(DateTime::from(
            Utc.timestamp_millis_opt(42_000_000).unwrap(),
        ))],
    );
    assert_eq!(f, Field::String("1970-01-01T11:40:00+00:00".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS STRING) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::UInt,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::UInt(42)],
    );
    assert_eq!(f, Field::String("42".to_string()));
}

#[test]
fn test_text() {
    // let f = run_scalar_fct(
    //     "SELECT CAST(field AS STRING) FROM users",
    //     Schema::default()
    //         .field(
    //             FieldDefinition::new(String::from("field"), FieldType::Binary, false),
    //             false,
    //         )
    //         .clone(),
    //     vec![Field::Binary(vec![])],
    // );
    // assert_eq!(f, Field::String("".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS TEXT) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Boolean,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Boolean(true)],
    );
    assert_eq!(f, Field::Text("TRUE".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS TEXT) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Date,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Date(NaiveDate::from_ymd_opt(2022, 1, 1).unwrap())],
    );
    assert_eq!(f, Field::Text("2022-01-01".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS TEXT) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Decimal,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Decimal(Decimal::new(42, 1))],
    );
    assert_eq!(f, Field::Text("4.2".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS TEXT) FROM users",
        Schema::default()
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
        vec![Field::Float(OrderedFloat(4.2))],
    );
    assert_eq!(f, Field::Text("4.2".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS TEXT) FROM users",
        Schema::default()
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
        vec![Field::Int(-42)],
    );
    assert_eq!(f, Field::Text("-42".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS TEXT) FROM users",
        Schema::default()
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
        vec![Field::String("Hello".to_string())],
    );
    assert_eq!(f, Field::Text("Hello".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS TEXT) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Text,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Text("Hello".to_string())],
    );
    assert_eq!(f, Field::Text("Hello".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS TEXT) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Timestamp(DateTime::from(
            Utc.timestamp_millis_opt(42_000_000).unwrap(),
        ))],
    );
    assert_eq!(f, Field::Text("1970-01-01T11:40:00+00:00".to_string()));

    let f = run_fct(
        "SELECT CAST(field AS TEXT) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::UInt,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::UInt(42)],
    );
    assert_eq!(f, Field::Text("42".to_string()));
}

#[test]
fn test_date() {
    let date = "2023-11-22";
    let date_time = DateTime::parse_from_rfc3339(&format!("{}T10:00:00+00:00", date)).unwrap();
    let f = run_fct(
        "SELECT CAST(field AS DATE) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Timestamp(date_time)],
    );

    assert_eq!(
        f,
        Field::Date(NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap())
    );
}
