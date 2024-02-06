use crate::connector::map_value_to_field;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use dozer_ingestion_connector::dozer_types::ordered_float::OrderedFloat;
use dozer_ingestion_connector::dozer_types::rust_decimal::Decimal;
use dozer_ingestion_connector::dozer_types::serde_json::{json, Value};
use dozer_ingestion_connector::dozer_types::types::{Field, FieldType};

#[macro_export]
macro_rules! test_conversion {
    ($a:expr,$b:expr,$c:expr,$d:expr) => {
        assert_eq!(map_value_to_field($a, $b, $c).unwrap(), $d)
    };
}

#[test]
pub fn test_type_conversion() {
    assert_eq!(
        map_value_to_field("str", Value::Null, FieldType::UInt).unwrap(),
        Field::Null
    );

    test_conversion!("bool", Value::Bool(true), FieldType::UInt, Field::UInt(1));
    test_conversion!("bool", Value::Bool(true), FieldType::U128, Field::U128(1));
    test_conversion!("bool", Value::Bool(true), FieldType::Int, Field::Int(1));
    test_conversion!("bool", Value::Bool(true), FieldType::I128, Field::I128(1));
    test_conversion!(
        "bool",
        Value::Bool(true),
        FieldType::Float,
        Field::Float(OrderedFloat(1.0))
    );
    test_conversion!(
        "bool",
        Value::Bool(true),
        FieldType::Boolean,
        Field::Boolean(true)
    );
    test_conversion!(
        "bool",
        Value::Bool(true),
        FieldType::String,
        Field::String("true".to_string())
    );
    test_conversion!(
        "bool",
        Value::Bool(true),
        FieldType::Text,
        Field::Text("true".to_string())
    );
    test_conversion!(
        "bool",
        Value::Bool(true),
        FieldType::Binary,
        Field::Binary(vec![8, 1])
    );
    test_conversion!(
        "bool",
        Value::Bool(true),
        FieldType::Decimal,
        Field::Decimal(Decimal::from(1))
    );

    test_conversion!("number", json!(30), FieldType::UInt, Field::UInt(30));
    test_conversion!("number", json!(31), FieldType::U128, Field::U128(31));
    test_conversion!("number", json!(32), FieldType::Int, Field::Int(32));
    test_conversion!("number", json!(33), FieldType::I128, Field::I128(33));
    test_conversion!(
        "number",
        json!(34.35),
        FieldType::Float,
        Field::Float(OrderedFloat(34.35))
    );
    test_conversion!("number", json!(1), FieldType::Boolean, Field::Boolean(true));
    test_conversion!(
        "number",
        json!(0),
        FieldType::Boolean,
        Field::Boolean(false)
    );
    test_conversion!(
        "number",
        json!(36),
        FieldType::String,
        Field::String("36".to_string())
    );
    test_conversion!(
        "number",
        json!(37),
        FieldType::Text,
        Field::Text("37".to_string())
    );
    test_conversion!(
        "number",
        json!(38),
        FieldType::Binary,
        Field::Binary(vec![51, 56])
    );

    test_conversion!("str", json!("40"), FieldType::UInt, Field::UInt(40));
    test_conversion!("str", json!("41"), FieldType::U128, Field::U128(41));
    test_conversion!("str", json!("42"), FieldType::Int, Field::Int(42));
    test_conversion!("str", json!("43"), FieldType::I128, Field::I128(43));
    test_conversion!(
        "str",
        json!("44.45"),
        FieldType::Float,
        Field::Float(OrderedFloat(44.45))
    );
    test_conversion!("str", json!("1"), FieldType::Boolean, Field::Boolean(true));
    test_conversion!(
        "str",
        json!("true"),
        FieldType::Boolean,
        Field::Boolean(true)
    );
    test_conversion!("str", json!("0"), FieldType::Boolean, Field::Boolean(false));
    test_conversion!(
        "str",
        json!("47"),
        FieldType::String,
        Field::String("47".to_string())
    );
    test_conversion!(
        "str",
        json!("48"),
        FieldType::Text,
        Field::Text("48".to_string())
    );
    test_conversion!(
        "str",
        json!(BASE64_STANDARD.encode(b"49")),
        FieldType::Binary,
        Field::Binary(vec![52, 57])
    );
}
