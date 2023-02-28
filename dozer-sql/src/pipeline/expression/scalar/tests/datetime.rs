use crate::pipeline::expression::scalar::tests::scalar_common::run_scalar_fct;
use dozer_types::chrono::{DateTime, NaiveDate};
use dozer_types::types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition};

#[test]
fn test_date() {
    let f = run_scalar_fct(
        "SELECT DAY_OF_WEEK(date) FROM users",
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
        vec![Field::Date(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap())],
    );
    assert_eq!(f, Field::Int(6));
}

#[test]
fn test_timestamp() {
    let f = run_scalar_fct(
        "SELECT DAY_OF_WEEK(ts) FROM users",
        Schema::empty()
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
        vec![Field::Timestamp(
            DateTime::parse_from_rfc3339("2023-01-02T00:12:10Z").unwrap(),
        )],
    );
    assert_eq!(f, Field::Int(0));
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
    assert_eq!(f, Field::Int(1000));
}

// #[test]
// fn test_timestamp_add() {
//     let f = run_scalar_fct(
//         "SELECT ts1 + ts2 FROM users",
//         Schema::empty()
//             .field(
//                 FieldDefinition::new(
//                     String::from("ts1"),
//                     FieldType::Timestamp,
//                     false,
//                     SourceDefinition::Dynamic,
//                 ),
//                 false,
//             )
//             .field(
//                 FieldDefinition::new(
//                     String::from("ts2"),
//                     FieldType::Timestamp,
//                     false,
//                     SourceDefinition::Dynamic,
//                 ),
//                 false,
//             )
//             .clone(),
//         vec![
//             Field::Timestamp(DateTime::parse_from_rfc3339("1970-01-01T00:00:10Z").unwrap()),
//             Field::Timestamp(DateTime::parse_from_rfc3339("1970-01-01T00:00:10Z").unwrap()),
//         ],
//     );
//     assert_eq!(f, Field::Int(20000));
// }

// #[test]
// fn test_timestamp_mul() {
//     let f = run_scalar_fct(
//         "SELECT ts1 * ts2 FROM users",
//         Schema::empty()
//             .field(
//                 FieldDefinition::new(
//                     String::from("ts1"),
//                     FieldType::Timestamp,
//                     false,
//                     SourceDefinition::Dynamic,
//                 ),
//                 false,
//             )
//             .field(
//                 FieldDefinition::new(
//                     String::from("ts2"),
//                     FieldType::Timestamp,
//                     false,
//                     SourceDefinition::Dynamic,
//                 ),
//                 false,
//             )
//             .clone(),
//         vec![
//             Field::Timestamp(DateTime::parse_from_rfc3339("1970-01-01T00:00:10Z").unwrap()),
//             Field::Timestamp(DateTime::parse_from_rfc3339("1970-01-01T00:00:10Z").unwrap()),
//         ],
//     );
//     assert_eq!(f, Field::Int(100000000));
// }
