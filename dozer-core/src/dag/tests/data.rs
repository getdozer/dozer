use crate::dag::tests::sources::FnOpGen;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};

pub(crate) fn get_schema() -> Schema {
    Schema::empty()
        .field(
            FieldDefinition::new("id".to_string(), FieldType::String, false),
            true,
            true,
        )
        .field(
            FieldDefinition::new("value".to_string(), FieldType::String, false),
            true,
            false,
        )
        .clone()
}

pub(crate) fn get_record_gen() -> &'static FnOpGen {
    &|n: u64| Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::String(format!("key_{}", n)),
                Field::String(format!("value_{}", n)),
            ],
        ),
    }
}
