use crate::pipeline::expression::tests::test_common::run_fct;
use dozer_types::json_types::{serde_json_to_json_value, JsonValue};
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
        Schema::empty()
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
        Schema::empty()
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
        Schema::empty()
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
        Schema::empty()
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
        Schema::empty()
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
    assert_eq!(f, Field::Json(serde_json_to_json_value(json!([
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
    ])).unwrap()));
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
        Schema::empty()
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
    assert_eq!(f, Field::Json(serde_json_to_json_value(json!([
        30,
        31,
    ])).unwrap()));
}
