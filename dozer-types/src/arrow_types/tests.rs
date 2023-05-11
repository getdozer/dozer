use arrow::datatypes::{self as arrow_types};
use std::collections::HashMap;

use crate::arrow_types::from_arrow::map_schema_to_dozer;
use crate::arrow_types::to_arrow::{DOZER_SCHEMA_KEY};
use crate::types::{FieldDefinition, FieldType, Schema as DozerSchema, SourceDefinition};

#[test]
fn can_convert_from_arrow_to_dozer() {
    let field_a = arrow_types::Field::new("a", arrow_types::DataType::Int64, false);
    let field_b = arrow_types::Field::new("b", arrow_types::DataType::Boolean, false);
    let field_c = arrow_types::Field::new("c", arrow_types::DataType::Utf8, false);
    let field_d = arrow_types::Field::new("d", arrow_types::DataType::Utf8, false);

    let schema = arrow_types::Schema::new(vec![field_a.clone(), field_b, field_c, field_d]);
    let dozer_schema = DozerSchema::empty()
        .field(FieldDefinition::new(
            "a".to_string(),
            FieldType::Int,
            false,
            SourceDefinition::Dynamic,
        ), true)
        .field(FieldDefinition::new(
                "b".to_string(),
                FieldType::Boolean,
                false,
                SourceDefinition::Dynamic,
            ), false)
        .field(
            FieldDefinition::new(
                "c".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ), false)
        .field(
            FieldDefinition::new(
                "d".to_string(),
                FieldType::Json,
                false,
                SourceDefinition::Dynamic,
            ), false)
        .clone();

    let mut metadata: HashMap<String, String> = HashMap::new();
    metadata.insert("row_count".to_string(), "100".to_string());
    metadata.insert(DOZER_SCHEMA_KEY.to_string(), serde_json::to_string(&dozer_schema).expect("Schema can always be serialized as JSON"));

    let dozer_schema = map_schema_to_dozer(&schema.with_metadata(metadata)).unwrap();
    assert_eq!(
        dozer_schema.fields[0].name,
        field_a.name().to_string().as_str()
    );
}
