use dozer_types::types::{FieldDefinition, FieldType, Schema};

pub fn get_input_schema() -> Schema {
    Schema::empty()
        .field(
            FieldDefinition::new("City".to_string(), FieldType::String, false),
            true,
            false,
        )
        .field(
            FieldDefinition::new("Region".to_string(), FieldType::String, false),
            true,
            false,
        )
        .field(
            FieldDefinition::new("Country".to_string(), FieldType::String, false),
            true,
            false,
        )
        .field(
            FieldDefinition::new("People".to_string(), FieldType::Int, false),
            true,
            false,
        )
        .clone()
}

pub fn get_expected_schema() -> Schema {
    Schema::empty()
        .field(
            FieldDefinition::new("City".to_string(), FieldType::String, false),
            true,
            true,
        )
        .field(
            FieldDefinition::new("Country".to_string(), FieldType::String, false),
            true,
            true,
        )
        .field(
            FieldDefinition::new("People".to_string(), FieldType::Int, false),
            true,
            true,
        )
        .clone()
}