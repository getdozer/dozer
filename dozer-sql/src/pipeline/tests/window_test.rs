use dozer_types::{
    chrono::{DateTime, Duration},
    types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition},
};

use crate::pipeline::window::{HopWindow, TumbleWindow, WindowFunction};

#[test]
fn test_hop() {
    let record = Record::new(
        None,
        vec![
            Field::Int(0),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap()),
        ],
        Some(1),
    );

    let window = HopWindow::new(1, Duration::minutes(1), Duration::minutes(5));
    let result = window.execute(&record).unwrap();
    assert_eq!(result.len(), 5);
    let window_record = result.get(0).unwrap();

    let expected_record = Record::new(
        None,
        vec![
            Field::Int(0),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap()),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:08:00Z").unwrap()),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap()),
        ],
        Some(1),
    );

    assert_eq!(*window_record, expected_record);

    let window_record = result.get(1).unwrap();

    let expected_record = Record::new(
        None,
        vec![
            Field::Int(0),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap()),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:09:00Z").unwrap()),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:14:00Z").unwrap()),
        ],
        Some(1),
    );

    assert_eq!(*window_record, expected_record);
}

#[test]
fn test_tumble() {
    let record = Record::new(
        None,
        vec![
            Field::Int(0),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap()),
        ],
        Some(1),
    );

    let window = TumbleWindow::new(1, Duration::minutes(5));
    let result = window.execute(&record).unwrap();
    assert_eq!(result.len(), 1);
    let window_record = result.get(0).unwrap();

    let expected_record = Record::new(
        None,
        vec![
            Field::Int(0),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap()),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:10:00Z").unwrap()),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:15:00Z").unwrap()),
        ],
        Some(1),
    );

    assert_eq!(*window_record, expected_record);
}

#[test]
fn test_window_schema() {
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                String::from("id"),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            true,
        )
        .field(
            FieldDefinition::new(
                String::from("timestamp"),
                FieldType::Timestamp,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();

    let window = TumbleWindow::new(3, Duration::seconds(10));
    let result = window.get_output_schema(&schema).unwrap();

    let expected_schema = Schema::empty()
        .field(
            FieldDefinition::new(
                String::from("id"),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            true,
        )
        .field(
            FieldDefinition::new(
                String::from("timestamp"),
                FieldType::Timestamp,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                String::from("window_start"),
                FieldType::Timestamp,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                String::from("window_end"),
                FieldType::Timestamp,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();

    assert_eq!(result, expected_schema);
}
