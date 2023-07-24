use crate::pipeline::expression::datetime::evaluate_date_part;
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::mathematical::{
    evaluate_add, evaluate_div, evaluate_mod, evaluate_mul, evaluate_sub,
};
use crate::pipeline::expression::tests::test_common::*;
use dozer_types::chrono;
use dozer_types::chrono::{DateTime, Datelike, NaiveDate};
use dozer_types::types::{
    DozerDuration, Field, FieldDefinition, FieldType, ProcessorRecord, Schema, SourceDefinition,
    TimeUnit,
};
use num_traits::ToPrimitive;
use proptest::prelude::*;
use sqlparser::ast::DateTimeField;

#[test]
fn test_time() {
    proptest!(
        ProptestConfig::with_cases(1000),
        move |(datetime: ArbitraryDateTime)| {
            test_date_parts(datetime)
    });
}

fn test_date_parts(datetime: ArbitraryDateTime) {
    let row = ProcessorRecord::new();

    let date_parts = vec![
        (
            DateTimeField::Dow,
            datetime
                .0
                .weekday()
                .num_days_from_monday()
                .to_i64()
                .unwrap(),
        ),
        (DateTimeField::Year, datetime.0.year().to_i64().unwrap()),
        (DateTimeField::Month, datetime.0.month().to_i64().unwrap()),
        (DateTimeField::Hour, 0),
        (DateTimeField::Second, 0),
        (
            DateTimeField::Quarter,
            datetime.0.month0().to_i64().map(|m| m / 3 + 1).unwrap(),
        ),
    ];

    let v = Expression::Literal(Field::Date(datetime.0.date_naive()));

    for (part, value) in date_parts {
        let result = evaluate_date_part(&Schema::default(), &part, &v, &row).unwrap();
        assert_eq!(result, Field::Int(value));
    }
}

#[test]
fn test_extract_date() {
    let date_fns: Vec<(&str, i64, i64)> = vec![
        ("dow", 6, 0),
        ("day", 1, 2),
        ("month", 1, 1),
        ("year", 2023, 2023),
        ("hour", 0, 0),
        ("minute", 0, 12),
        ("second", 0, 10),
        ("millisecond", 1672531200000, 1672618330000),
        ("microsecond", 1672531200000000, 1672618330000000),
        ("nanoseconds", 1672531200000000000, 1672618330000000000),
        ("quarter", 1, 1),
        ("epoch", 1672531200, 1672618330),
        ("week", 52, 1),
        ("century", 21, 21),
        ("decade", 203, 203),
        ("doy", 1, 2),
    ];
    let inputs = vec![
        Field::Date(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()),
        Field::Timestamp(DateTime::parse_from_rfc3339("2023-01-02T00:12:10Z").unwrap()),
    ];

    for (part, val1, val2) in date_fns {
        let mut results = vec![];
        for i in inputs.clone() {
            let f = run_fct(
                &format!("select extract({part} from date) from users"),
                Schema::default()
                    .field(
                        FieldDefinition::new(
                            String::from("date"),
                            FieldType::Date,
                            false,
                            SourceDefinition::Dynamic,
                        ),
                        false,
                    )
                    .clone(),
                vec![i.clone()],
            );
            results.push(f.to_int().unwrap());
        }
        assert_eq!(val1, results[0]);
        assert_eq!(val2, results[1]);
    }
}

#[test]
fn test_timestamp_diff() {
    let f = run_fct(
        "SELECT ts1 - ts2 FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("ts1"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("ts2"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![
            Field::Timestamp(DateTime::parse_from_rfc3339("2023-01-02T00:12:11Z").unwrap()),
            Field::Timestamp(DateTime::parse_from_rfc3339("2023-01-02T00:12:10Z").unwrap()),
        ],
    );
    assert_eq!(
        f,
        Field::Duration(DozerDuration(
            std::time::Duration::from_secs(1),
            TimeUnit::Nanoseconds
        ))
    );
}

#[test]
fn test_duration() {
    proptest!(
        ProptestConfig::with_cases(1000),
        move |(d1: u64, d2: u64, dt1: ArbitraryDateTime)| {
            test_duration_math(d1, d2, dt1)
    });
}

fn test_duration_math(d1: u64, d2: u64, dt1: ArbitraryDateTime) {
    let row = ProcessorRecord::new();

    let v = Expression::Literal(Field::Date(dt1.0.date_naive()));
    let dur1 = Expression::Literal(Field::Duration(DozerDuration(
        std::time::Duration::from_nanos(d1),
        TimeUnit::Nanoseconds,
    )));
    let dur2 = Expression::Literal(Field::Duration(DozerDuration(
        std::time::Duration::from_nanos(d2),
        TimeUnit::Nanoseconds,
    )));

    // Duration + Duration = Duration
    let result = evaluate_add(&Schema::default(), &dur1, &dur2, &row);
    let sum = std::time::Duration::from_nanos(d1).checked_add(std::time::Duration::from_nanos(d2));
    if result.is_ok() && sum.is_some() {
        assert_eq!(
            result.unwrap(),
            Field::Duration(DozerDuration(sum.unwrap(), TimeUnit::Nanoseconds))
        );
    }
    // Duration - Duration = Duration
    let result = evaluate_sub(&Schema::default(), &dur1, &dur2, &row);
    let diff = std::time::Duration::from_nanos(d1).checked_sub(std::time::Duration::from_nanos(d2));
    if result.is_ok() && diff.is_some() {
        assert_eq!(
            result.unwrap(),
            Field::Duration(DozerDuration(diff.unwrap(), TimeUnit::Nanoseconds))
        );
    }
    // Duration * Duration = Error
    let result = evaluate_mul(&Schema::default(), &dur1, &dur2, &row);
    assert!(result.is_err());
    // Duration / Duration = Error
    let result = evaluate_div(&Schema::default(), &dur1, &dur2, &row);
    assert!(result.is_err());
    // Duration % Duration = Error
    let result = evaluate_mod(&Schema::default(), &dur1, &dur2, &row);
    assert!(result.is_err());

    // Duration + Timestamp = Error
    let result = evaluate_add(&Schema::default(), &dur1, &v, &row);
    assert!(result.is_err());
    // Duration - Timestamp = Error
    let result = evaluate_sub(&Schema::default(), &dur1, &v, &row);
    assert!(result.is_err());
    // Duration * Timestamp = Error
    let result = evaluate_mul(&Schema::default(), &dur1, &v, &row);
    assert!(result.is_err());
    // Duration / Timestamp = Error
    let result = evaluate_div(&Schema::default(), &dur1, &v, &row);
    assert!(result.is_err());
    // Duration % Timestamp = Error
    let result = evaluate_mod(&Schema::default(), &dur1, &v, &row);
    assert!(result.is_err());

    // Timestamp + Duration = Timestamp
    let result = evaluate_add(&Schema::default(), &v, &dur1, &row);
    let sum = dt1
        .0
        .checked_add_signed(chrono::Duration::nanoseconds(d1 as i64));
    if result.is_ok() && sum.is_some() {
        assert_eq!(result.unwrap(), Field::Timestamp(sum.unwrap()));
    }
    // Timestamp - Duration = Timestamp
    let result = evaluate_sub(&Schema::default(), &v, &dur2, &row);
    let diff = dt1
        .0
        .checked_sub_signed(chrono::Duration::nanoseconds(d2 as i64));
    if result.is_ok() && diff.is_some() {
        assert_eq!(result.unwrap(), Field::Timestamp(diff.unwrap()));
    }
    // Timestamp * Duration = Error
    let result = evaluate_mul(&Schema::default(), &v, &dur1, &row);
    assert!(result.is_err());
    // Timestamp / Duration = Error
    let result = evaluate_div(&Schema::default(), &v, &dur1, &row);
    assert!(result.is_err());
    // Timestamp % Duration = Error
    let result = evaluate_mod(&Schema::default(), &v, &dur1, &row);
    assert!(result.is_err());
}

#[test]
fn test_interval() {
    let f = run_fct(
        "SELECT ts1 - INTERVAL '1' SECOND FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("ts1"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Timestamp(
            DateTime::parse_from_rfc3339("2023-01-02T00:12:11Z").unwrap(),
        )],
    );
    assert_eq!(
        f,
        Field::Timestamp(DateTime::parse_from_rfc3339("2023-01-02T00:12:10Z").unwrap())
    );

    let f = run_fct(
        "SELECT ts1 + INTERVAL '1' SECOND FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("ts1"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Timestamp(
            DateTime::parse_from_rfc3339("2023-01-02T00:12:11Z").unwrap(),
        )],
    );
    assert_eq!(
        f,
        Field::Timestamp(DateTime::parse_from_rfc3339("2023-01-02T00:12:12Z").unwrap())
    );

    let f = run_fct(
        "SELECT INTERVAL '1' SECOND + ts1 FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("ts1"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Timestamp(
            DateTime::parse_from_rfc3339("2023-01-02T00:12:11Z").unwrap(),
        )],
    );
    assert_eq!(
        f,
        Field::Timestamp(DateTime::parse_from_rfc3339("2023-01-02T00:12:12Z").unwrap())
    );
}

#[test]
fn test_now() {
    let f = run_fct(
        "SELECT NOW() FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("ts1"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![],
    );
    assert!(f.to_timestamp().is_ok())
}
