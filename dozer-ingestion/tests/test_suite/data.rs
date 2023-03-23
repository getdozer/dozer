use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SchemaIdentifier,
};

pub fn append_only_operations_without_primary_key() -> (Schema, Vec<Operation>) {
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

    let operations = vec![Operation::Insert {
        new: Record::new(schema.identifier, vec![Field::UInt(0)], None),
    }];

    (schema, operations)
}

pub fn append_only_operations_with_primary_key() -> (Schema, Vec<Operation>) {
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

    let operations = vec![Operation::Insert {
        new: Record::new(schema.identifier, vec![Field::UInt(0)], None),
    }];

    (schema, operations)
}

pub fn all_kinds_of_operations() -> (Schema, Vec<Operation>) {
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

    let operations = vec![Operation::Insert {
        new: Record::new(schema.identifier, vec![Field::UInt(0)], None),
    }];

    (schema, operations)
}
