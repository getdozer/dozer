use dozer_core::processor_record::{ProcessorRecord, ProcessorRecordStore};
use dozer_types::types::Record;
use dozer_types::{
    chrono::{DateTime, Duration},
    types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition},
};

use crate::pipeline::window::operator::WindowType;

#[test]
fn test_hop() {
    let record_store = ProcessorRecordStore::new().unwrap();
    let record = record_store
        .create_record(&Record::new(vec![
            Field::Int(0),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap()),
        ]))
        .unwrap();

    let window = WindowType::Hop {
        column_index: 1,
        hop_size: Duration::minutes(1),
        interval: Duration::minutes(5),
    };
    let result = window
        .execute(
            &record_store,
            record.clone(),
            record_store.load_record(&record).unwrap(),
        )
        .unwrap();
    assert_eq!(result.len(), 5);
    let window_record = result.get(0).unwrap();

    let expected_record = ProcessorRecord::appended(
        &record,
        record_store
            .create_ref(&[
                Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:09:00Z").unwrap()),
                Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:14:00Z").unwrap()),
            ])
            .unwrap(),
    );

    assert_eq!(window_record, &expected_record);

    let window_record = result.get(1).unwrap();

    let expected_record = ProcessorRecord::appended(
        &record,
        record_store
            .create_ref(&[
                Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:10:00Z").unwrap()),
                Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:15:00Z").unwrap()),
            ])
            .unwrap(),
    );

    assert_eq!(window_record, &expected_record);
}

#[test]
fn test_tumble() {
    let record_store = ProcessorRecordStore::new().unwrap();
    let record = record_store
        .create_record(&Record::new(vec![
            Field::Int(0),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap()),
        ]))
        .unwrap();

    let window = WindowType::Tumble {
        column_index: 1,
        interval: Duration::minutes(5),
    };

    let result = window
        .execute(
            &record_store,
            record.clone(),
            record_store.load_record(&record).unwrap(),
        )
        .unwrap();
    assert_eq!(result.len(), 1);
    let window_record = result.get(0).unwrap();

    let expected_record = ProcessorRecord::appended(
        &record,
        record_store
            .create_ref(&[
                Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:10:00Z").unwrap()),
                Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:15:00Z").unwrap()),
            ])
            .unwrap(),
    );

    assert_eq!(window_record, &expected_record);
}

#[test]
fn test_window_schema() {
    let schema = Schema::default()
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

    let window = WindowType::Tumble {
        column_index: 3,
        interval: Duration::seconds(10),
    };

    let result = window.get_output_schema(&schema).unwrap();

    let mut expected_schema = Schema::default()
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

    expected_schema.primary_index = vec![0, 2];

    assert_eq!(result, expected_schema);
}
