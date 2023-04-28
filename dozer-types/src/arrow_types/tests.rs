use arrow::datatypes::{self as arrow_types};
use std::collections::HashMap;

use crate::arrow_types::from_arrow::map_schema_to_dozer;
use crate::arrow_types::to_arrow::{JSON_TYPE, LOGICAL_TYPE_KEY};

#[test]
fn can_convert_from_arrow_to_dozer() {
    let field_a = arrow_types::Field::new("a", arrow_types::DataType::Int64, false);
    let field_b = arrow_types::Field::new("b", arrow_types::DataType::Boolean, false);
    let field_c = arrow_types::Field::new("c", arrow_types::DataType::Utf8, false);
    let mut field_d = arrow_types::Field::new("d", arrow_types::DataType::Utf8, false);
    field_d = field_d.with_metadata(HashMap::from([(
        LOGICAL_TYPE_KEY.to_string(),
        JSON_TYPE.to_string(),
    )]));

    let mut metadata: HashMap<String, String> = HashMap::new();
    metadata.insert("row_count".to_string(), "100".to_string());

    let schema = arrow_types::Schema::new_with_metadata(
        vec![field_a.clone(), field_b, field_c, field_d],
        metadata,
    );

    let dozer_schema = map_schema_to_dozer(&schema).unwrap();
    assert_eq!(
        dozer_schema.fields[0].name,
        field_a.name().to_string().as_str()
    );
}
