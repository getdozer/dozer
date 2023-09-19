use crate::expression::tests::test_common::*;
use dozer_types::chrono::{DateTime, NaiveDate};
use dozer_types::types::{
    DozerDuration, Field, FieldDefinition, FieldType, Schema, SourceDefinition, TimeUnit,
};

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
    assert!(f.to_timestamp().is_some())
}
