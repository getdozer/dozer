use std::time::Duration;

use crate::connector::map_value_to_field;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use dozer_ingestion_connector::dozer_types::ordered_float::OrderedFloat;
use dozer_ingestion_connector::dozer_types::rust_decimal::Decimal;
use dozer_ingestion_connector::dozer_types::serde_json::{json, Value};
use dozer_ingestion_connector::dozer_types::types::{Field, FieldType};

#[test]
pub fn test_type_conversion() {
    assert_eq!(
        map_value_to_field("str", Value::Null, FieldType::UInt).unwrap(),
        Field::Null
    );

    assert_eq!(
        map_value_to_field("bool", Value::Bool(true), FieldType::Boolean).unwrap(),
        Field::Boolean(true)
    );
    assert!(map_value_to_field("str", Value::String("hello".into()), FieldType::Boolean).is_err());

    assert_eq!(
        map_value_to_field("int", json!(30), FieldType::UInt).unwrap(),
        (Field::UInt(30))
    );
    assert_eq!(
        map_value_to_field("int", json!(30), FieldType::Int).unwrap(),
        (Field::Int(30))
    );
    assert!(map_value_to_field("float", json!(30), FieldType::UInt).is_err());

    assert_eq!(
        map_value_to_field("float", json!(34.35), FieldType::Float).unwrap(),
        (Field::Float(OrderedFloat(34.35)))
    );

    assert_eq!(
        map_value_to_field("float", json!(30), FieldType::Float).unwrap(),
        (Field::Float(OrderedFloat(30.)))
    );
    assert!(map_value_to_field("int", json!(1), FieldType::Float).is_err());

    assert_eq!(
        map_value_to_field("str", json!("47"), FieldType::String).unwrap(),
        Field::String("47".to_string())
    );
    assert_eq!(
        map_value_to_field("str", json!("48"), FieldType::Text).unwrap(),
        Field::Text("48".to_string())
    );

    assert_eq!(
        map_value_to_field(
            "blob",
            json!(BASE64_STANDARD.encode(vec![52, 57])),
            FieldType::Binary
        )
        .unwrap(),
        Field::Binary(vec![52, 57])
    );

    assert_eq!(
        map_value_to_field("str", json!("30.42"), FieldType::Decimal).unwrap(),
        Field::Decimal(Decimal::new(3042, 2))
    );

    assert_eq!(
        map_value_to_field("str", json!("PT3.0012S"), FieldType::Duration).unwrap(),
        Field::Duration(
            dozer_ingestion_connector::dozer_types::types::DozerDuration(
                Duration::new(3, 1_200_000),
                dozer_ingestion_connector::dozer_types::types::TimeUnit::Nanoseconds
            )
        )
    );
}
