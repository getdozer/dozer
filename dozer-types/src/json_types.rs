use crate::errors::types::SerializationError;
use crate::types::{DozerDuration, Field, DATE_FORMAT};
use chrono::SecondsFormat;
use ordered_float::OrderedFloat;
use prost_types::value::Kind;
use prost_types::{ListValue, Struct, Value as ProstValue};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord, Hash)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(OrderedFloat<f64>),
    String(String),
    Array(Vec<JsonValue>),
    Object(BTreeMap<String, JsonValue>),
}

impl Display for JsonValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonValue::Null => f.write_str("NULL"),
            JsonValue::Bool(v) => f.write_str(&format!("{v}")),
            JsonValue::Number(v) => f.write_str(&format!("{v}")),
            JsonValue::String(v) => f.write_str(&v.to_string()),
            JsonValue::Array(v) => {
                let list: Vec<String> = v.iter().map(|val| format!("{val}")).collect();
                let data = &format!("[{}]", list.join(","));
                f.write_str(data)
            }
            JsonValue::Object(v) => {
                let list: Vec<String> = v.iter().map(|(key, val)| format!("{key}:{val}")).collect();
                let data = &format!("{{ {} }}", list.join(","));
                f.write_str(data)
            }
        }
    }
}

// todo: for serde_json conversion
impl FromStr for JsonValue {
    type Err = SerializationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let object: Value = serde_json::from_str(s).unwrap();
        Ok(serde_json_to_json_value(object))
    }
}

fn convert_x_y_to_object((x, y): &(OrderedFloat<f64>, OrderedFloat<f64>)) -> Value {
    let mut m = Map::new();
    m.insert("x".to_string(), Value::from(x.0));
    m.insert("y".to_string(), Value::from(y.0));
    Value::Object(m)
}

fn convert_duration_to_object(d: &DozerDuration) -> Value {
    let mut m = Map::new();
    m.insert("value".to_string(), Value::from(d.0.as_nanos().to_string()));
    m.insert("time_unit".to_string(), Value::from(d.1.to_string()));
    Value::Object(m)
}

/// Should be consistent with `convert_cache_type_to_schema_type`.
pub fn field_to_json_value(field: Field) -> Value {
    match field {
        Field::UInt(n) => Value::from(n),
        Field::U128(n) => Value::String(n.to_string()),
        Field::Int(n) => Value::from(n),
        Field::I128(n) => Value::String(n.to_string()),
        Field::Float(n) => Value::from(n.0),
        Field::Boolean(b) => Value::from(b),
        Field::String(s) => Value::from(s),
        Field::Text(n) => Value::from(n),
        Field::Binary(b) => Value::from(b),
        Field::Decimal(n) => Value::String(n.to_string()),
        Field::Timestamp(ts) => Value::String(ts.to_rfc3339_opts(SecondsFormat::Millis, true)),
        Field::Date(n) => Value::String(n.format(DATE_FORMAT).to_string()),
        Field::Json(b) => json_value_to_serde_json(b),
        Field::Point(point) => convert_x_y_to_object(&point.0.x_y()),
        Field::Duration(d) => convert_duration_to_object(&d),
        Field::Null => Value::Null,
    }
}

pub fn json_value_to_serde_json(value: JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Bool(b),
        JsonValue::Number(n) => Value::Number(Number::from_f64(*n).unwrap()),
        JsonValue::String(s) => Value::String(s),
        JsonValue::Array(a) => Value::Array(
            a.iter()
                .map(|val| json_value_to_serde_json(val.to_owned()))
                .collect(),
        ),
        JsonValue::Object(o) => {
            let mut values: Map<String, Value> = Map::new();
            for (key, val) in o {
                values.insert(key, json_value_to_serde_json(val));
            }
            Value::Object(values)
        }
    }
}

pub fn json_value_to_prost_kind(val: JsonValue) -> ProstValue {
    ProstValue {
        kind: match val {
            JsonValue::Null => Some(Kind::NullValue(0)),
            JsonValue::Bool(b) => Some(Kind::BoolValue(b)),
            JsonValue::Number(n) => Some(Kind::NumberValue(*n)),
            JsonValue::String(s) => Some(Kind::StringValue(s)),
            JsonValue::Array(a) => {
                let values: prost::alloc::vec::Vec<ProstValue> = a
                    .iter()
                    .map(|val| json_value_to_prost_kind(val.to_owned()))
                    .collect();
                Some(Kind::ListValue(ListValue { values }))
            }
            JsonValue::Object(o) => {
                let fields: prost::alloc::collections::BTreeMap<
                    prost::alloc::string::String,
                    ProstValue,
                > = o
                    .iter()
                    .map(|(key, val)| {
                        (
                            prost::alloc::string::String::from(key),
                            json_value_to_prost_kind(val.to_owned()),
                        )
                    })
                    .collect();
                Some(Kind::StructValue(Struct { fields }))
            }
        },
    }
}

// todo: not sure whether we need to involve serde_json conversion, and From<serde_json> for try_get from row
pub fn serde_json_to_json_value(value: Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(b) => JsonValue::Bool(b),
        Value::Number(n) => JsonValue::Number(OrderedFloat(n.as_f64().unwrap())),
        Value::String(s) => JsonValue::String(s),
        Value::Array(a) => JsonValue::Array(
            a.iter()
                .map(|val| serde_json_to_json_value(val.to_owned()))
                .collect(),
        ),
        Value::Object(o) => {
            let mut values: BTreeMap<String, JsonValue> = BTreeMap::<String, JsonValue>::new();
            for (key, val) in o {
                values.insert(key, serde_json_to_json_value(val));
            }
            JsonValue::Object(values)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        chrono::{NaiveDate, Offset, TimeZone, Utc},
        json_value_to_field,
        ordered_float::OrderedFloat,
        rust_decimal::Decimal,
        types::{DozerPoint, Field, FieldType, TimeUnit},
    };

    use std::time::Duration;

    use super::*;

    fn test_field_conversion(field_type: FieldType, field: Field) {
        // Convert the field to a JSON value.
        let value = field_to_json_value(field.clone());

        // Convert the JSON value back to a Field.
        let deserialized = json_value_to_field(value, field_type, true).unwrap();

        assert_eq!(deserialized, field, "must be equal");
    }

    #[test]
    fn test_field_types_json_conversion() {
        let fields = vec![
            (FieldType::Int, Field::Int(-1)),
            (FieldType::UInt, Field::UInt(1)),
            (FieldType::Float, Field::Float(OrderedFloat(1.1))),
            (FieldType::Boolean, Field::Boolean(true)),
            (FieldType::String, Field::String("a".to_string())),
            (FieldType::Binary, Field::Binary(b"asdf".to_vec())),
            (FieldType::Decimal, Field::Decimal(Decimal::new(202, 2))),
            (
                FieldType::Timestamp,
                Field::Timestamp(Utc.fix().with_ymd_and_hms(2001, 1, 1, 0, 4, 0).unwrap()),
            ),
            (
                FieldType::Date,
                Field::Date(NaiveDate::from_ymd_opt(2022, 11, 24).unwrap()),
            ),
            (
                FieldType::Json,
                Field::Json(JsonValue::Array(vec![
                    JsonValue::Number(OrderedFloat(123_f64)),
                    JsonValue::Number(OrderedFloat(34_f64)),
                    JsonValue::Number(OrderedFloat(97_f64)),
                    JsonValue::Number(OrderedFloat(98_f64)),
                    JsonValue::Number(OrderedFloat(99_f64)),
                    JsonValue::Number(OrderedFloat(34_f64)),
                    JsonValue::Number(OrderedFloat(58_f64)),
                    JsonValue::Number(OrderedFloat(34_f64)),
                    JsonValue::Number(OrderedFloat(102_f64)),
                    JsonValue::Number(OrderedFloat(111_f64)),
                    JsonValue::Number(OrderedFloat(111_f64)),
                    JsonValue::Number(OrderedFloat(34_f64)),
                ])),
            ),
            (FieldType::Text, Field::Text("lorem ipsum".to_string())),
            (
                FieldType::Point,
                Field::Point(DozerPoint::from((3.234, 4.567))),
            ),
            (
                FieldType::Duration,
                Field::Duration(DozerDuration(
                    Duration::from_nanos(123_u64),
                    TimeUnit::Nanoseconds,
                )),
            ),
        ];
        for (field_type, field) in fields {
            test_field_conversion(field_type, field);
        }
    }
}
