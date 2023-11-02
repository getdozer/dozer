use std::cmp::Ordering;

use crate::errors::types::DeserializationError;
use crate::types::{DozerDuration, Field, DATE_FORMAT};
use chrono::SecondsFormat;
use ordered_float::OrderedFloat;
use prost_types::value::Kind;
use prost_types::{ListValue, Struct, Value as ProstValue};
use serde_json::{Map, Value};

use ijson::{Destructured, DestructuredRef, IArray, IObject, IValue};

pub type JsonValue = IValue;
pub type JsonObject = IObject;
pub type JsonArray = IArray;

pub type DestructuredJsonRef<'a> = DestructuredRef<'a>;
pub type DestructuredJson = Destructured;
pub use ijson::ijson as json;

pub fn parse_json(from: &str) -> Result<JsonValue, DeserializationError> {
    let serde_value: serde_json::Value = serde_json::from_str(from)?;
    ijson::to_value(serde_value).map_err(Into::into)
}

pub fn parse_json_slice(bytes: &[u8]) -> Result<JsonValue, DeserializationError> {
    let serde_value: serde_json::Value = serde_json::from_slice(bytes)?;
    ijson::to_value(serde_value).map_err(Into::into)
}

pub fn json_to_string(value: &JsonValue) -> String {
    // The debug implementation of IValue produces a json string, but this is
    // not a stable guarantee. Therefore, roundtrip through serde_json
    let serde_value = json_value_to_serde_json(value);
    serde_value.to_string()
}

pub(crate) fn json_to_bytes(value: &JsonValue) -> Vec<u8> {
    bson::to_vec(&json!({"data": value})).unwrap()
}

pub(crate) fn json_from_bytes(bytes: &[u8]) -> Result<JsonValue, DeserializationError> {
    let v: JsonValue = bson::from_slice(bytes)?;
    // We always wrap the value in an object for bson's sake, so safe to unwrap
    // Ideally, we'd use `JsonValue::into_object()`, but that's bugged
    Ok(v.as_object().unwrap().to_owned()["data"].clone())
}

pub fn json_to_bytes_size(value: &JsonValue) -> usize {
    json_to_bytes(value).len()
}

pub(crate) fn json_cmp(l: &JsonValue, r: &JsonValue) -> std::cmp::Ordering {
    // Early return in the common case
    if l.type_() != r.type_() {
        return l.type_().cmp(&r.type_());
    }

    match (l.destructure_ref(), r.destructure_ref()) {
        (DestructuredRef::Object(l), DestructuredRef::Object(r)) => {
            let mut l_sorted: Vec<_> = l.iter().collect();
            l_sorted.sort_by(|v0, v1| v0.0.cmp(v1.0));

            let mut r_sorted: Vec<_> = r.iter().collect();
            r_sorted.sort_by(|v0, v1| v0.0.cmp(v1.0).then_with(|| json_cmp(v0.1, v1.1)));

            for ((l_k, l_v), (r_k, r_v)) in l_sorted.into_iter().zip(r_sorted) {
                let cmp = l_k.cmp(r_k).then_with(|| json_cmp(l_v, r_v));
                if !cmp.is_eq() {
                    return cmp;
                }
            }
            l.len().cmp(&r.len())
        }
        (DestructuredRef::Array(l), DestructuredRef::Array(r)) => {
            for (l, r) in l.iter().zip(r) {
                match json_cmp(l, r) {
                    Ordering::Equal => (),
                    non_eq => return non_eq,
                }
            }
            l.len().cmp(&r.len())
        }
        (DestructuredRef::Null, DestructuredRef::Null) => Ordering::Equal,
        (DestructuredRef::Bool(l), DestructuredRef::Bool(r)) => l.cmp(&r),
        (DestructuredRef::Number(l), DestructuredRef::Number(r)) => l.cmp(r),
        (DestructuredRef::String(l), DestructuredRef::String(r)) => l.cmp(r),
        // We checked the types were equal before
        _ => unreachable!(),
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
        Field::Json(b) => json_value_to_serde_json(&b),
        Field::Point(point) => convert_x_y_to_object(&point.0.x_y()),
        Field::Duration(d) => convert_duration_to_object(&d),
        Field::Null => Value::Null,
    }
}

pub fn json_value_to_serde_json(value: &JsonValue) -> Value {
    // Note that while this cannot fail, the other way might, as our internal JSON
    // representation does not support `inf`, `-inf` and NaN
    ijson::from_value(value).expect("Json to Json conversion should never fail")
}

pub fn prost_to_json_value(val: ProstValue) -> JsonValue {
    match val.kind {
        Some(v) => match v {
            Kind::NullValue(_) => JsonValue::NULL,
            Kind::BoolValue(b) => b.into(),
            Kind::NumberValue(n) => n.into(),
            Kind::StringValue(s) => s.into(),
            Kind::ListValue(l) => l
                .values
                .into_iter()
                .map(prost_to_json_value)
                .collect::<IArray>()
                .into(),
            Kind::StructValue(s) => s
                .fields
                .into_iter()
                .map(|(key, val)| (key, prost_to_json_value(val)))
                .collect::<IObject>()
                .into(),
        },
        None => JsonValue::NULL,
    }
}

pub fn json_value_to_prost(val: JsonValue) -> ProstValue {
    ProstValue {
        kind: match val.destructure() {
            Destructured::Null => Some(Kind::NullValue(0)),
            Destructured::Bool(b) => Some(Kind::BoolValue(b)),
            Destructured::Number(n) => Some(Kind::NumberValue(n.to_f64_lossy())),
            Destructured::String(s) => Some(Kind::StringValue(s.into())),
            Destructured::Array(a) => {
                let values: prost::alloc::vec::Vec<ProstValue> =
                    a.into_iter().map(json_value_to_prost).collect();
                Some(Kind::ListValue(ListValue { values }))
            }
            Destructured::Object(o) => {
                let fields: prost::alloc::collections::BTreeMap<
                    prost::alloc::string::String,
                    ProstValue,
                > = o
                    .into_iter()
                    .map(|(key, val)| (key.into(), json_value_to_prost(val)))
                    .collect();
                Some(Kind::StructValue(Struct { fields }))
            }
        },
    }
}

pub fn serde_json_to_json_value(value: Value) -> Result<JsonValue, DeserializationError> {
    ijson::to_value(value).map_err(Into::into)
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

        assert_eq!(deserialized, field);
    }

    macro_rules! check_cmp {
        ($l:tt, $r:tt, $ordering:expr) => {
            assert_eq!(json_cmp(&json!($l), &json!($r)), $ordering);
            // Invertible
            assert_eq!(json_cmp(&json!($r), &json!($l)), $ordering.reverse());
        };
    }
    #[test]
    fn test_json_ord_object() {
        check_cmp!({"a": 2, "b": 3}, {"a": 2, "b": 2}, Ordering::Greater);
        check_cmp!({"a": 2, "b": 3}, {"a": 2, "b": 4}, Ordering::Less);
        check_cmp!({"a": 2, "b": 3}, {"a": 2, "b": 3}, Ordering::Equal);

        // Insertion order independent
        check_cmp!({"a": 2, "b": 3}, {"b": 2, "a": 2}, Ordering::Greater);
        check_cmp!({"a": 2, "b": 3}, {"b": 4, "a": 2}, Ordering::Less);
        check_cmp!({"a": 2, "b": 3}, {"b": 3, "a": 2}, Ordering::Equal);

        // Sorted key-value comparison
        check_cmp!({"b": 3}, {"a": 3, "b": 3}, Ordering::Greater);

        check_cmp!({"a": 2}, {"b": 2}, Ordering::Less);
        check_cmp!({}, {"b": 2}, Ordering::Less);
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
                Field::Json(
                    vec![
                        123_f64, 34_f64, 97_f64, 98_f64, 99_f64, 34_f64, 58_f64, 34_f64, 102_f64,
                        111_f64, 111_f64, 34_f64,
                    ]
                    .into(),
                ),
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
