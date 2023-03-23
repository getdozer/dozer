use crate::pipeline::expression::datetime::evaluate_date_part;
use crate::pipeline::expression::execution::Expression;
use dozer_types::chrono::{
    DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike,
};

use crate::pipeline::expression::scalar::tests::scalar_common::run_scalar_fct;
use dozer_types::types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition};
use num_traits::ToPrimitive;
use proptest::prelude::*;
use sqlparser::ast::DateTimeField;

#[derive(Debug)]
struct ArbitraryDateTime(DateTime<FixedOffset>);

impl Arbitrary for ArbitraryDateTime {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (
            NaiveDateTime::MIN.year()..NaiveDateTime::MAX.year(),
            1..13u32,
            1..32u32,
            0..NaiveDateTime::MAX.second(),
            0..NaiveDateTime::MAX.nanosecond(),
        )
            .prop_map(|(year, month, day, secs, nano)| {
                let timezone_east = FixedOffset::east_opt(8 * 60 * 60).unwrap();
                let date = NaiveDate::from_ymd_opt(year, month, day);
                // Some dates are not able to created caused by leap in February with day larger than 28 or 29
                if date.is_none() {
                    return ArbitraryDateTime(DateTime::default());
                }
                let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nano).unwrap();
                let datetime = DateTime::<FixedOffset>::from_local(
                    NaiveDateTime::new(date.unwrap(), time),
                    timezone_east,
                );
                ArbitraryDateTime(datetime)
            })
            .boxed()
    }
}

#[test]
fn test_time() {
    proptest!(
        ProptestConfig::with_cases(1000),
        move |(datetime: ArbitraryDateTime)| {
            test_date_parts(datetime)
    });
}

fn test_date_parts(datetime: ArbitraryDateTime) {
    let row = Record::new(None, vec![], None);

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
        let result = evaluate_date_part(&Schema::empty(), &part, &v, &row).unwrap();
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
            let f = run_scalar_fct(
                &format!("select extract({part} from date) from users"),
                Schema::empty()
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
    let f = run_scalar_fct(
        "SELECT ts1 - ts2 FROM users",
        Schema::empty()
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
    assert_eq!(f, Field::Int(1000 * 1000 * 1000));
}

#[test]
fn test_timestamp_add() {
    let f = run_scalar_fct(
        "SELECT ts1 + ts2 FROM users",
        Schema::empty()
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
            Field::Timestamp(DateTime::parse_from_rfc3339("1970-01-01T00:00:10Z").unwrap()),
            Field::Timestamp(DateTime::parse_from_rfc3339("1970-01-01T00:00:10Z").unwrap()),
        ],
    );
    assert_eq!(f, Field::Int(20000));
}

#[test]
fn test_timestamp_mul() {
    let f = run_scalar_fct(
        "SELECT ts1 * ts2 FROM users",
        Schema::empty()
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
            Field::Timestamp(DateTime::parse_from_rfc3339("1970-01-01T00:00:10Z").unwrap()),
            Field::Timestamp(DateTime::parse_from_rfc3339("1970-01-01T00:00:10Z").unwrap()),
        ],
    );
    assert_eq!(f, Field::Int(100000000));
}
