use dozer_types::types::{Field, FieldDefinition, FieldType};

use super::FieldsAndPk;

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
    ];

    let records = vec![vec![Field::UInt(0), Field::Int(0)]];

    ((fields, vec![]), records)
}

pub fn records_with_primary_key() -> (FieldsAndPk, Vec<Vec<Field>>) {
    let ((fields, _), records) = records_without_primary_key();
    ((fields, vec![0]), records)
}
