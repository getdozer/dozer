use dozer_types::json_types::JsonValue;
use dozer_types::types::{Field, FieldDefinition, FieldType};

use super::{records::Operation, FieldsAndPk};

pub fn records_without_primary_key() -> (FieldsAndPk, Vec<Vec<Field>>) {
    let fields = vec![
        FieldDefinition {
            name: "uint".to_string(),
            typ: FieldType::UInt,
            nullable: false,
            source: Default::default(),
        },
        FieldDefinition {
            name: "int".to_string(),
            typ: FieldType::Int,
            nullable: false,
            source: Default::default(),
        },
        FieldDefinition {
            name: "string".to_string(),
            typ: FieldType::String,
            nullable: false,
            source: Default::default(),
        },
        FieldDefinition {
            name: "json".to_string(),
            typ: FieldType::Json,
            nullable: false,
            source: Default::default(),
        },
    ];

    let records = vec![vec![
        Field::UInt(0),
        Field::Int(0),
        Field::String(String::from("s")),
        Field::Json(JsonValue::String(String::from("s"))),
    ]];

    ((fields, vec![]), records)
}

pub fn records_with_primary_key() -> (FieldsAndPk, Vec<Vec<Field>>) {
    let ((fields, _), records) = records_without_primary_key();
    ((fields, vec![0]), records)
}

pub fn cud_operations() -> (FieldsAndPk, Vec<Operation>) {
    let (schema, records) = records_with_primary_key();
    let operations = vec![
        Operation::Insert {
            new: records[0].clone(),
        },
        Operation::Update {
            old: records[0].clone(),
            new: vec![Field::UInt(1), Field::Int(1)],
        },
        Operation::Delete {
            old: records[0].clone(),
        },
    ];
    (schema, operations)
}
