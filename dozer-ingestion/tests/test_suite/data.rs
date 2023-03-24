use dozer_types::types::{Field, FieldDefinition, FieldType, Record, Schema, SchemaIdentifier};

pub fn records_without_primary_key() -> (Schema, Vec<Record>) {
    let mut schema = Schema::empty();
    schema.identifier = Some(SchemaIdentifier { id: 0, version: 0 });
    schema.field(
        FieldDefinition {
            name: "uint".to_string(),
            typ: FieldType::UInt,
            nullable: false,
            source: Default::default(),
        },
        false,
    );

    let records = vec![Record::new(schema.identifier, vec![Field::UInt(0)], None)];

    (schema, records)
}

pub fn records_with_primary_key() -> (Schema, Vec<Record>) {
    let mut schema = Schema::empty();
    schema.identifier = Some(SchemaIdentifier { id: 0, version: 0 });
    schema.field(
        FieldDefinition {
            name: "uint".to_string(),
            typ: FieldType::UInt,
            nullable: false,
            source: Default::default(),
        },
        true,
    );

    let records = vec![Record::new(schema.identifier, vec![Field::UInt(0)], None)];

    (schema, records)
}
