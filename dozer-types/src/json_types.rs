use crate::errors::types::{CannotConvertF64ToJson, DeserializationError};
use crate::types::{DozerDuration, Field, DATE_FORMAT};
use chrono::SecondsFormat;
use ordered_float::OrderedFloat;
use prost_types::value::Kind;
use prost_types::{ListValue, Struct, Value as ProstValue};
use rust_decimal::prelude::FromPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(
        #[cfg_attr(feature="arbitrary", arbitrary(with = crate::types::field::arbitrary_float))]
        OrderedFloat<f64>,
    ),
    String(String),
    Array(Vec<JsonValue>),
    Object(BTreeMap<String, JsonValue>),
}

#[allow(clippy::derivable_impls)]
impl Default for JsonValue {
    fn default() -> JsonValue {
        JsonValue::Null
    }
}

impl JsonValue {
    pub fn as_array(&self) -> Option<&Vec<JsonValue>> {
        match self {
            JsonValue::Array(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&BTreeMap<String, JsonValue>> {
        match self {
            JsonValue::Object(map) => Some(map),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            JsonValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            JsonValue::Number(n) => Some(n.0 as i64),
            _ => None,
        }
    }

    pub fn as_i128(&self) -> Option<i128> {
        match self {
            JsonValue::Number(n) => i128::from_f64(n.0),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            JsonValue::Number(n) => Some(n.0 as u64),
            _ => None,
        }
    }

    pub fn as_u128(&self) -> Option<u128> {
        match self {
            JsonValue::Number(n) => u128::from_f64(n.0),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            JsonValue::Number(n) => Some(n.0),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match *self {
            JsonValue::Bool(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_null(&self) -> Option<()> {
        match *self {
            JsonValue::Null => Some(()),
            _ => None,
        }
    }
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

impl FromStr for JsonValue {
    type Err = DeserializationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let object = serde_json::from_str(s);
        if object.is_ok() {
            serde_json_to_json_value(object?)
        } else {
            let f = OrderedFloat::from_str(s)
                .map_err(|e| DeserializationError::Custom(Box::from(format!("{:?}", e))));
            if f.is_ok() {
                Ok(JsonValue::Number(f?))
            } else {
                let b = bool::from_str(s)
                    .map_err(|e| DeserializationError::Custom(Box::from(format!("{:?}", e))));
                if b.is_ok() {
                    Ok(JsonValue::Bool(b?))
                } else {
                    Ok(JsonValue::String(String::from(s)))
                }
            }
        }
    }
}

impl From<usize> for JsonValue {
    fn from(f: usize) -> Self {
        From::from(f as f64)
    }
}

impl From<f32> for JsonValue {
    fn from(f: f32) -> Self {
        From::from(f as f64)
    }
}

impl From<f64> for JsonValue {
    fn from(f: f64) -> Self {
        JsonValue::Number(OrderedFloat(f))
    }
}

impl From<bool> for JsonValue {
    fn from(f: bool) -> Self {
        JsonValue::Bool(f)
    }
}

impl From<String> for JsonValue {
    fn from(f: String) -> Self {
        JsonValue::String(f)
    }
}

impl<'a> From<&'a str> for JsonValue {
    fn from(f: &str) -> Self {
        JsonValue::String(f.to_string())
    }
}

impl<'a> From<Cow<'a, str>> for JsonValue {
    fn from(f: Cow<'a, str>) -> Self {
        JsonValue::String(f.into_owned())
    }
}

impl From<OrderedFloat<f64>> for JsonValue {
    fn from(f: OrderedFloat<f64>) -> Self {
        JsonValue::Number(f)
    }
}

impl From<BTreeMap<String, JsonValue>> for JsonValue {
    fn from(f: BTreeMap<String, JsonValue>) -> Self {
        JsonValue::Object(f)
    }
}

impl<T: Into<JsonValue>> From<Vec<T>> for JsonValue {
    fn from(f: Vec<T>) -> Self {
        JsonValue::Array(f.into_iter().map(Into::into).collect())
    }
}

impl<'a, T: Clone + Into<JsonValue>> From<&'a [T]> for JsonValue {
    fn from(f: &'a [T]) -> Self {
        JsonValue::Array(f.iter().cloned().map(Into::into).collect())
    }
}

impl<T: Into<JsonValue>> FromIterator<T> for JsonValue {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        JsonValue::Array(iter.into_iter().map(Into::into).collect())
    }
}

impl<K: Into<String>, V: Into<JsonValue>> FromIterator<(K, V)> for JsonValue {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        JsonValue::Object(
            iter.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

impl From<()> for JsonValue {
    fn from((): ()) -> Self {
        JsonValue::Null
    }
}

impl<T> From<Option<T>> for JsonValue
where
    T: Into<JsonValue>,
{
    fn from(opt: Option<T>) -> Self {
        match opt {
            None => JsonValue::Null,
            Some(value) => Into::into(value),
        }
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
pub fn field_to_json_value(field: Field) -> Result<Value, CannotConvertF64ToJson> {
    match field {
        Field::UInt(n) => Ok(Value::from(n)),
        Field::U128(n) => Ok(Value::String(n.to_string())),
        Field::Int(n) => Ok(Value::from(n)),
        Field::I128(n) => Ok(Value::String(n.to_string())),
        Field::Float(n) => Ok(Value::from(n.0)),
        Field::Boolean(b) => Ok(Value::from(b)),
        Field::String(s) => Ok(Value::from(s)),
        Field::Text(n) => Ok(Value::from(n)),
        Field::Binary(b) => Ok(Value::from(b)),
        Field::Decimal(n) => Ok(Value::String(n.to_string())),
        Field::Timestamp(ts) => Ok(Value::String(
            ts.to_rfc3339_opts(SecondsFormat::Millis, true),
        )),
        Field::Date(n) => Ok(Value::String(n.format(DATE_FORMAT).to_string())),
        Field::Json(b) => json_value_to_serde_json(b),
        Field::Point(point) => Ok(convert_x_y_to_object(&point.0.x_y())),
        Field::Duration(d) => Ok(convert_duration_to_object(&d)),
        Field::Null => Ok(Value::Null),
    }
}

pub fn json_value_to_serde_json(value: JsonValue) -> Result<Value, CannotConvertF64ToJson> {
    match value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(b) => Ok(Value::Bool(b)),
        JsonValue::Number(n) => match Number::from_f64(n.0) {
            Some(number) => Ok(Value::Number(number)),
            None => Err(CannotConvertF64ToJson(n.0)),
        },
        JsonValue::String(s) => Ok(Value::String(s)),
        JsonValue::Array(a) => {
            let mut lst: Vec<Value> = vec![];
            for val in a {
                lst.push(json_value_to_serde_json(val)?);
            }
            Ok(Value::Array(lst))
        }
        JsonValue::Object(o) => {
            let mut values: Map<String, Value> = Map::new();
            for (key, val) in o {
                values.insert(key, json_value_to_serde_json(val)?);
            }
            Ok(Value::Object(values))
        }
    }
}

pub fn prost_to_json_value(val: ProstValue) -> JsonValue {
    match val.kind {
        Some(v) => match v {
            Kind::NullValue(_) => JsonValue::Null,
            Kind::BoolValue(b) => JsonValue::Bool(b),
            Kind::NumberValue(n) => JsonValue::Number(OrderedFloat(n)),
            Kind::StringValue(s) => JsonValue::String(s),
            Kind::ListValue(l) => {
                JsonValue::Array(l.values.into_iter().map(prost_to_json_value).collect())
            }
            Kind::StructValue(s) => JsonValue::Object(
                s.fields
                    .into_iter()
                    .map(|(key, val)| (key, prost_to_json_value(val)))
                    .collect(),
            ),
        },
        None => JsonValue::Null,
    }
}

pub fn json_value_to_prost(val: JsonValue) -> ProstValue {
    ProstValue {
        kind: match val {
            JsonValue::Null => Some(Kind::NullValue(0)),
            JsonValue::Bool(b) => Some(Kind::BoolValue(b)),
            JsonValue::Number(n) => Some(Kind::NumberValue(*n)),
            JsonValue::String(s) => Some(Kind::StringValue(s)),
            JsonValue::Array(a) => {
                let values: prost::alloc::vec::Vec<ProstValue> =
                    a.into_iter().map(json_value_to_prost).collect();
                Some(Kind::ListValue(ListValue { values }))
            }
            JsonValue::Object(o) => {
                let fields: prost::alloc::collections::BTreeMap<
                    prost::alloc::string::String,
                    ProstValue,
                > = o
                    .into_iter()
                    .map(|(key, val)| (key, json_value_to_prost(val)))
                    .collect();
                Some(Kind::StructValue(Struct { fields }))
            }
        },
    }
}

pub fn serde_json_to_json_value(value: Value) -> Result<JsonValue, DeserializationError> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Bool(b) => Ok(JsonValue::Bool(b)),
        Value::Number(n) => Ok(JsonValue::Number(OrderedFloat(match n.as_f64() {
            Some(f) => f,
            None => return Err(DeserializationError::F64TypeConversionError(n)),
        }))),
        Value::String(s) => Ok(JsonValue::String(s)),
        Value::Array(a) => {
            let mut lst = vec![];
            for val in a {
                lst.push(serde_json_to_json_value(val)?);
            }
            Ok(JsonValue::Array(lst))
        }
        Value::Object(o) => {
            let mut values: BTreeMap<String, JsonValue> = BTreeMap::<String, JsonValue>::new();
            for (key, val) in o {
                values.insert(key, serde_json_to_json_value(val)?);
            }
            Ok(JsonValue::Object(values))
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
        let deserialized = json_value_to_field(value.unwrap(), field_type, true).unwrap();

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
