use dozer_types::chrono::{DateTime, Utc};
use dozer_types::types::{Field, Record};
use dozer_types::types::{FieldDefinition, FieldType, Schema, SchemaIdentifier, SourceDefinition};
use opentelemetry::sdk::export::trace::SpanData;

pub(crate) fn map_span_data(span_data: SpanData) -> (Record, Vec<Record>) {
    let start_time: DateTime<Utc> = span_data.start_time.into();
    let end_time: DateTime<Utc> = span_data.end_time.into();

    let span_id = u64::from_be_bytes(span_data.span_context.span_id().to_bytes());
    let span_record = Record {
        schema_id: Some(SchemaIdentifier { id: 1, version: 1 }),
        values: vec![
            Field::UInt(span_id),
            Field::Binary(span_data.span_context.trace_id().to_bytes().to_vec()),
            Field::Text(span_data.name.to_string()),
            Field::UInt(u64::from_be_bytes(span_data.parent_span_id.to_bytes())),
            Field::Timestamp(start_time.into()),
            Field::Timestamp(end_time.into()),
        ],
    };

    let mut events = vec![];
    for evt in span_data.events {
        let ts: DateTime<Utc> = evt.timestamp.into();
        let record = Record {
            schema_id: Some(SchemaIdentifier { id: 2, version: 1 }),
            values: vec![
                Field::UInt(span_id),
                Field::Text(evt.name.to_string()),
                Field::Timestamp(ts.into()),
            ],
        };

        events.push(record);
    }
    (span_record, events)
}

pub fn spans_schema() -> Schema {
    let fields = vec![
        FieldDefinition {
            name: "id".to_string(),
            typ: FieldType::UInt,
            nullable: false,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "trace_id".to_string(),
            typ: FieldType::Binary,
            nullable: false,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "name".to_string(),
            typ: FieldType::Text,
            nullable: false,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "parent_id".to_string(),
            typ: FieldType::UInt,
            nullable: true,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "start_time".to_string(),
            typ: FieldType::Timestamp,
            nullable: false,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "end_time".to_string(),
            typ: FieldType::Timestamp,
            nullable: true,
            source: SourceDefinition::Dynamic,
        },
    ];

    Schema {
        identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
        fields,
        primary_index: vec![0],
    }
}

pub fn events_schema() -> Schema {
    let fields = vec![
        FieldDefinition {
            name: "span_id".to_string(),
            typ: FieldType::UInt,
            nullable: false,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "name".to_string(),
            typ: FieldType::Text,
            nullable: false,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "timestamp".to_string(),
            typ: FieldType::Timestamp,
            nullable: false,
            source: SourceDefinition::Dynamic,
        },
    ];

    Schema {
        identifier: Some(SchemaIdentifier { id: 2, version: 1 }),
        fields,
        primary_index: vec![],
    }
}
