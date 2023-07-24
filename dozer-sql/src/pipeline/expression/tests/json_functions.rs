use crate::pipeline::expression::tests::test_common::run_fct;
use dozer_types::json_types::{serde_json_to_json_value, JsonValue};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::serde_json::json;
use dozer_types::types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition};
use std::collections::BTreeMap;

#[test]
fn test_json_value() {
    let json_val = serde_json_to_json_value(json!(
        {
            "info":{
                "type":1,
                "address":{
                    "town":"Bristol",
                    "county":"Avon",
                    "country":"England"
                },
                "tags":["Sport", "Water polo"]
            },
            "type":"Basic"
        }
    ))
    .unwrap();

    let f = run_fct(
        "SELECT JSON_VALUE(jsonInfo,'$.info.address.town') FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val)],
    );

    assert_eq!(f, Field::Json(JsonValue::String(String::from("Bristol"))));
}

#[test]
fn test_json_value_null() {
    let json_val = serde_json_to_json_value(json!(
        {
            "info":{
                "type":1,
                "address":{
                    "town":"Bristol",
                    "county":"Avon",
                    "country":"England"
                },
                "tags":["Sport", "Water polo"]
            },
            "type":"Basic"
        }
    ))
    .unwrap();

    let f = run_fct(
        "SELECT JSON_VALUE(jsonInfo,'$.info.address.tags') FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val)],
    );

    assert_eq!(f, Field::Json(JsonValue::Null));
}

#[test]
fn test_json_query() {
    let json_val = serde_json_to_json_value(json!(
        {
            "info": {
                "type": 1,
                "address": {
                    "town": "Cheltenham",
                    "county": "Gloucestershire",
                    "country": "England"
                },
                "tags": ["Sport", "Water polo"]
            },
            "type": "Basic"
        }
    ))
    .unwrap();

    let f = run_fct(
        "SELECT JSON_QUERY(jsonInfo,'$.info.address') FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val)],
    );

    assert_eq!(
        f,
        Field::Json(JsonValue::Object(BTreeMap::from([
            (
                "town".to_string(),
                JsonValue::String("Cheltenham".to_string())
            ),
            (
                "county".to_string(),
                JsonValue::String("Gloucestershire".to_string())
            ),
            (
                "country".to_string(),
                JsonValue::String("England".to_string())
            ),
        ])))
    );
}

#[test]
fn test_json_query_null() {
    let json_val = serde_json_to_json_value(json!(
        {
            "info": {
                "type": 1,
                "address": {
                    "town": "Cheltenham",
                    "county": "Gloucestershire",
                    "country": "England"
                },
                "tags": ["Sport", "Water polo"]
            },
            "type": "Basic"
        }
    ))
    .unwrap();

    let f = run_fct(
        "SELECT JSON_QUERY(jsonInfo,'$.type') FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val)],
    );
    assert_eq!(f, Field::Json(JsonValue::String("Basic".to_string())));
}

#[test]
fn test_json_query_len_one_array() {
    let json_val = serde_json_to_json_value(json!(
        {
            "info": {
                "type": 1,
                "address": {
                    "town": "Cheltenham",
                    "county": "Gloucestershire",
                    "country": "England"
                },
                "tags": ["Sport"]
            },
            "type": "Basic"
        }
    ))
    .unwrap();

    let f = run_fct(
        "SELECT JSON_VALUE(jsonInfo,'$.info.tags') FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val)],
    );
    assert_eq!(
        f,
        Field::Json(JsonValue::Array(vec![JsonValue::String(String::from(
            "Sport"
        ))]))
    );
}

#[test]
fn test_json_query_array() {
    let json_val = serde_json_to_json_value(json!(
        {
            "info": {
                "type": 1,
                "address": {
                    "town": "Cheltenham",
                    "county": "Gloucestershire",
                    "country": "England"
                },
                "tags": ["Sport", "Water polo"]
            },
            "type": "Basic"
        }
    ))
    .unwrap();

    let f = run_fct(
        "SELECT JSON_QUERY(jsonInfo,'$.info.tags') FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val)],
    );

    assert_eq!(
        f,
        Field::Json(JsonValue::Array(vec![
            JsonValue::String(String::from("Sport")),
            JsonValue::String(String::from("Water polo")),
        ]))
    );
}

#[test]
fn test_json_query_default_path() {
    let json_val = serde_json_to_json_value(json!(
        {
            "Cities": [
                {
                    "Name": "Kabul",
                    "CountryCode": "AFG",
                    "District": "Kabol",
                    "Population": 1780000
                },
                {
                    "Name": "Qandahar",
                    "CountryCode": "AFG",
                    "District": "Qandahar",
                    "Population": 237500
                }
            ]
        }
    ))
    .unwrap();

    let f = run_fct(
        "SELECT JSON_QUERY(jsonInfo) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val.clone())],
    );
    assert_eq!(f, Field::Json(json_val));
}

#[test]
fn test_json_query_all() {
    let json_val = serde_json_to_json_value(json!(
        [
            {"digit": 30, "letter": "A"},
            {"digit": 31, "letter": "B"}
        ]
    ))
    .unwrap();

    let f = run_fct(
        "SELECT JSON_QUERY(jsonInfo, '$..*') FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val)],
    );

    assert_eq!(
        f,
        Field::Json(
            serde_json_to_json_value(json!([
                {
                    "digit": 30,
                    "letter": "A"
                },
                30,
                "A",
                {
                    "digit": 31,
                    "letter": "B"
                },
                31,
                "B"
            ]))
            .unwrap()
        )
    );
}

#[test]
fn test_json_query_iter() {
    let json_val = serde_json_to_json_value(json!(
        [
            {"digit": 30, "letter": "A"},
            {"digit": 31, "letter": "B"}
        ]
    ))
    .unwrap();

    let f = run_fct(
        "SELECT JSON_QUERY(jsonInfo, '$[*].digit') FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val)],
    );

    assert_eq!(
        f,
        Field::Json(serde_json_to_json_value(json!([30, 31,])).unwrap())
    );
}

#[test]
fn test_json_cast() {
    let f = run_fct(
        "SELECT CAST(uint AS JSON) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("uint"),
                    FieldType::UInt,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::UInt(10_u64)],
    );

    assert_eq!(f, Field::Json(JsonValue::Number(OrderedFloat(10_f64))));

    let f = run_fct(
        "SELECT CAST(u128 AS JSON) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("u128"),
                    FieldType::U128,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::U128(10_u128)],
    );

    assert_eq!(f, Field::Json(JsonValue::Number(OrderedFloat(10_f64))));

    let f = run_fct(
        "SELECT CAST(int AS JSON) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("int"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Int(10_i64)],
    );

    assert_eq!(f, Field::Json(JsonValue::Number(OrderedFloat(10_f64))));

    let f = run_fct(
        "SELECT CAST(i128 AS JSON) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("i128"),
                    FieldType::I128,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::I128(10_i128)],
    );

    assert_eq!(f, Field::Json(JsonValue::Number(OrderedFloat(10_f64))));

    let f = run_fct(
        "SELECT CAST(float AS JSON) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("float"),
                    FieldType::Float,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Float(OrderedFloat(10_f64))],
    );

    assert_eq!(f, Field::Json(JsonValue::Number(OrderedFloat(10_f64))));

    let f = run_fct(
        "SELECT CAST(str AS JSON) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("str"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::String("Dozer".to_string())],
    );

    assert_eq!(f, Field::Json(JsonValue::String("Dozer".to_string())));

    let f = run_fct(
        "SELECT CAST(str AS JSON) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("str"),
                    FieldType::Text,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Text("Dozer".to_string())],
    );

    assert_eq!(f, Field::Json(JsonValue::String("Dozer".to_string())));

    let f = run_fct(
        "SELECT CAST(bool AS JSON) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("bool"),
                    FieldType::Boolean,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Boolean(true)],
    );

    assert_eq!(f, Field::Json(JsonValue::Bool(true)));
}

#[test]
fn test_json_value_cast() {
    let json_val = serde_json_to_json_value(json!(
        [
            {"digit": 30, "letter": "A"},
            {"digit": 31, "letter": "B"}
        ]
    ))
    .unwrap();

    let f = run_fct(
        "SELECT JSON_VALUE(jsonInfo, '$[0].digit') FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val.clone())],
    );

    assert_eq!(f, Field::Json(JsonValue::Number(OrderedFloat(30_f64))));

    let f = run_fct(
        "SELECT CAST(JSON_VALUE(jsonInfo, '$[0].digit') AS UINT) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val.clone())],
    );

    assert_eq!(f, Field::UInt(30_u64));

    let f = run_fct(
        "SELECT CAST(JSON_VALUE(jsonInfo, '$[0].digit') AS INT) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val.clone())],
    );

    assert_eq!(f, Field::Int(30_i64));

    let f = run_fct(
        "SELECT CAST(JSON_VALUE(jsonInfo, '$[0].digit') AS FLOAT) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val.clone())],
    );

    assert_eq!(f, Field::Float(OrderedFloat(30_f64)));

    let f = run_fct(
        "SELECT CAST(JSON_VALUE(jsonInfo, '$[0].digit') AS STRING) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val)],
    );

    assert_eq!(f, Field::String("30".to_string()));

    let json_val = serde_json_to_json_value(json!(
        [
            {"bool": true},
            {"digit": 31, "letter": "B"}
        ]
    ))
    .unwrap();

    let f = run_fct(
        "SELECT CAST(JSON_VALUE(jsonInfo, '$[0].bool') AS BOOLEAN) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("jsonInfo"),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Json(json_val)],
    );

    assert_eq!(f, Field::Boolean(true));
}
