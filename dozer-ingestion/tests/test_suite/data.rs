use dozer_ingestion_connector::dozer_types::types::{Field, FieldDefinition, FieldType};

use super::{records::Operation, FieldsAndPk};

pub fn records_without_primary_key() -> (FieldsAndPk, Vec<Vec<Field>>) {
    let fields = vec![
        FieldDefinition {
            name: "int".to_string(),
            typ: FieldType::Int,
            nullable: false,
            source: Default::default(),
        },
        FieldDefinition {
            name: "uint".to_string(),
            typ: FieldType::UInt,
            nullable: false,
            source: Default::default(),
        },
    ];

    let records = vec![vec![Field::Int(0), Field::UInt(0)]];

    ((fields, vec![]), records)
}

pub fn records_with_primary_key() -> (FieldsAndPk, Vec<Vec<Field>>) {
    let ((fields, _), records) = records_without_primary_key();
    ((fields, vec![0]), records)
}

pub fn cud_operations() -> (FieldsAndPk, Vec<Operation>) {
    let (schema, records) = records_with_primary_key();
    let updated_record = vec![Field::Int(1), Field::UInt(1)];
    let operations = vec![
        Operation::Insert {
            new: records[0].clone(),
        },
        Operation::Update {
            old: records[0].clone(),
            new: updated_record.clone(),
        },
        Operation::Delete {
            old: updated_record,
        },
    ];
    (schema, operations)
}

pub fn reorder(
    fields: &[FieldDefinition],
    pk: &[usize],
    operations: &[Operation],
) -> (FieldsAndPk, Vec<Operation>) {
    let reversed_fields = fields.iter().rev().cloned().collect();
    let reversed_pk = pk.iter().map(|pk| fields.len() - 1 - pk).collect();

    let mut reversed_operations = operations.to_vec();
    for op in reversed_operations.iter_mut() {
        match op {
            Operation::Insert { new } => new.reverse(),
            Operation::Update { old, new } => {
                old.reverse();
                new.reverse();
            }
            Operation::Delete { old } => old.reverse(),
        }
    }
    ((reversed_fields, reversed_pk), reversed_operations)
}
