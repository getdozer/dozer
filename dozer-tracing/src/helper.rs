use std::collections::HashMap;

use dozer_types::types::{FieldDefinition, FieldType, Schema, SchemaIdentifier, SourceDefinition};

pub(crate) fn get_schemas() -> HashMap<u32, (String, Schema)> {
    let mut schemas = HashMap::new();
    schemas.insert(1, ("span".to_string(), span_schema()));
    schemas.insert(2, ("event".to_string(), event_schema()));
    schemas
}

fn span_schema() -> Schema {
    let mut fields = vec![
        FieldDefinition {
            name: "id".to_string(),
            typ: FieldType::UInt,
            nullable: false,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "parent_id".to_string(),
            typ: FieldType::UInt,
            nullable: true,
            source: SourceDefinition::Dynamic,
        },
    ];

    fields.extend(metadata_fields());
    fields.extend([
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
        FieldDefinition {
            name: "duration".to_string(),
            typ: FieldType::UInt,
            nullable: true,
            source: SourceDefinition::Dynamic,
        },
    ]);

    Schema {
        identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
        fields,
        primary_index: vec![0],
    }
}

fn event_schema() -> Schema {
    let mut fields = vec![FieldDefinition {
        name: "parent_id".to_string(),
        typ: FieldType::UInt,
        nullable: true,
        source: SourceDefinition::Dynamic,
    }];
    fields.extend(metadata_fields());

    Schema {
        identifier: Some(SchemaIdentifier { id: 2, version: 1 }),
        fields,
        primary_index: vec![],
    }
}

fn metadata_fields() -> Vec<FieldDefinition> {
    vec![
        FieldDefinition {
            name: "name".to_string(),
            typ: FieldType::String,
            nullable: false,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "target".to_string(),
            typ: FieldType::String,
            nullable: true,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "level".to_string(),
            typ: FieldType::String,
            nullable: false,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "module".to_string(),
            typ: FieldType::String,
            nullable: true,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "file".to_string(),
            typ: FieldType::String,
            nullable: true,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "line".to_string(),
            typ: FieldType::UInt,
            nullable: true,
            source: SourceDefinition::Dynamic,
        },
    ]
}
