use crate::errors::types::{DeserializationError, TypeError};
use crate::json_types::{json_from_str, JsonValue};
use crate::types::{DozerDuration, DozerPoint, TimeUnit, DATE_FORMAT};
use crate::types::{Field, FieldType};
use chrono::{DateTime, NaiveDate};
use geo::Point;
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::ops::Deref;
use std::str::FromStr;
use std::time::Duration;

/// Used in REST APIs and query expressions for converting JSON value to `Field`
pub fn json_value_to_field(
    value: JsonValue,
    typ: FieldType,
    nullable: bool,
) -> Result<Field, TypeError> {
    if nullable && value.is_null() {
        return Ok(Field::Null);
    }

    match typ {
        FieldType::UInt => {
            let Some(value) = value.as_number().and_then(|value| value.to_u64()) else {
                return Err(DeserializationError::JsonType(typ, value).into());
            };
            Ok(Field::UInt(value))
        }
        FieldType::Int => {
            let Some(value) = value.as_number().and_then(|value| value.to_i64()) else {
                return Err(DeserializationError::JsonType(typ, value).into());
            };
            Ok(Field::Int(value))
        }
        FieldType::Float => {
            let Some(value) = value.as_number().and_then(|value| value.to_f64()) else {
                return Err(DeserializationError::JsonType(typ, value).into());
            };
            Ok(Field::Float(OrderedFloat(value)))
        }
        FieldType::Boolean => {
            let Some(boolean) = value.to_bool() else {
                return Err(DeserializationError::JsonType(typ, value).into());
            };
            Ok(Field::Boolean(boolean))
        }
        FieldType::U128
        | FieldType::I128
        | FieldType::String
        | FieldType::Text
        | FieldType::Decimal
        | FieldType::Timestamp
        | FieldType::Date => {
            let Some(string) = value.as_string() else {
                return Err(DeserializationError::JsonType(typ, value).into());
            };
            Field::from_str(string.deref(), typ, nullable)
        }
        FieldType::Binary => {
            let Some(array) = value.as_array() else {
                return Err(DeserializationError::JsonType(typ, value).into());
            };
            let mut bytes = Vec::new();
            for item in array {
                let Some(number) = item
                    .as_number()
                    .and_then(|item| item.to_u32())
                    .and_then(|item| u8::try_from(item).ok())
                else {
                    return Err(DeserializationError::JsonType(typ, value).into());
                };
                bytes.push(number);
            }
            Ok(Field::Binary(bytes))
        }
        FieldType::Json => Ok(Field::Json(value)),
        FieldType::Point => {
            let Some(object) = value.as_object() else {
                return Err(DeserializationError::JsonType(typ, value).into());
            };
            let Some(x) = object.get("x").and_then(|x| x.to_f64()) else {
                return Err(DeserializationError::JsonType(typ, value).into());
            };
            let Some(y) = object.get("y").and_then(|y| y.to_f64()) else {
                return Err(DeserializationError::JsonType(typ, value).into());
            };
            Ok(Field::Point(DozerPoint(Point::new(
                OrderedFloat(x),
                OrderedFloat(y),
            ))))
        }
        FieldType::Duration => {
            let Some(object) = value.as_object() else {
                return Err(DeserializationError::JsonType(typ, value).into());
            };
            let Some(nanos) = object
                .get("value")
                .and_then(|value| value.as_string())
                .and_then(|value| value.parse().ok())
            else {
                return Err(DeserializationError::JsonType(typ, value).into());
            };
            let duration = Duration::from_nanos(nanos);
            let Some(time_unit) = object
                .get("time_unit")
                .and_then(|time_unit| time_unit.as_string())
                .and_then(|time_unit| TimeUnit::from_str(time_unit).ok())
            else {
                return Err(DeserializationError::JsonType(typ, value).into());
            };
            Ok(Field::Duration(DozerDuration(duration, time_unit)))
        }
    }
}

impl Field {
    pub fn from_str(value: &str, typ: FieldType, nullable: bool) -> Result<Field, TypeError> {
        match typ {
            FieldType::UInt => {
                if nullable && (value.is_empty() || value == "null") {
                    Ok(Field::Null)
                } else {
                    value.parse::<u64>().map(Field::UInt).map_err(|_| {
                        TypeError::InvalidFieldValue {
                            field_type: typ,
                            nullable,
                            value: value.to_string(),
                        }
                    })
                }
            }
            FieldType::U128 => {
                if nullable && (value.is_empty() || value == "null") {
                    Ok(Field::Null)
                } else {
                    value.parse::<u128>().map(Field::U128).map_err(|_| {
                        TypeError::InvalidFieldValue {
                            field_type: typ,
                            nullable,
                            value: value.to_string(),
                        }
                    })
                }
            }
            FieldType::Int => {
                if nullable && (value.is_empty() || value == "null") {
                    Ok(Field::Null)
                } else {
                    value
                        .parse::<i64>()
                        .map(Field::Int)
                        .map_err(|_| TypeError::InvalidFieldValue {
                            field_type: typ,
                            nullable,
                            value: value.to_string(),
                        })
                }
            }
            FieldType::I128 => {
                if nullable && (value.is_empty() || value == "null") {
                    Ok(Field::Null)
                } else {
                    value.parse::<i128>().map(Field::I128).map_err(|_| {
                        TypeError::InvalidFieldValue {
                            field_type: typ,
                            nullable,
                            value: value.to_string(),
                        }
                    })
                }
            }
            FieldType::Float => {
                if nullable && (value.is_empty() || value == "null") {
                    Ok(Field::Null)
                } else {
                    value
                        .parse::<OrderedFloat<f64>>()
                        .map(Field::Float)
                        .map_err(|_| TypeError::InvalidFieldValue {
                            field_type: typ,
                            nullable,
                            value: value.to_string(),
                        })
                }
            }
            FieldType::Boolean => {
                if nullable && (value.is_empty() || value == "null") {
                    Ok(Field::Null)
                } else {
                    value.parse::<bool>().map(Field::Boolean).map_err(|_| {
                        TypeError::InvalidFieldValue {
                            field_type: typ,
                            nullable,
                            value: value.to_string(),
                        }
                    })
                }
            }
            FieldType::String => Ok(Field::String(value.to_string())),
            FieldType::Text => Ok(Field::Text(value.to_string())),
            FieldType::Binary => {
                if nullable && (value.is_empty() || value == "null") {
                    Ok(Field::Null)
                } else {
                    Err(TypeError::InvalidFieldValue {
                        field_type: typ,
                        nullable,
                        value: value.to_string(),
                    })
                }
            }
            FieldType::Decimal => {
                if nullable && (value.is_empty() || value == "null") {
                    Ok(Field::Null)
                } else {
                    Decimal::from_str(value).map(Field::Decimal).map_err(|_| {
                        TypeError::InvalidFieldValue {
                            field_type: typ,
                            nullable,
                            value: value.to_string(),
                        }
                    })
                }
            }
            FieldType::Timestamp => {
                if nullable && (value.is_empty() || value == "null") {
                    Ok(Field::Null)
                } else {
                    DateTime::parse_from_rfc3339(value)
                        .map(Field::Timestamp)
                        .map_err(|_| TypeError::InvalidFieldValue {
                            field_type: typ,
                            nullable,
                            value: value.to_string(),
                        })
                }
            }
            FieldType::Date => {
                if nullable && (value.is_empty() || value == "null") {
                    Ok(Field::Null)
                } else {
                    NaiveDate::parse_from_str(value, DATE_FORMAT)
                        .map(Field::Date)
                        .map_err(|_| TypeError::InvalidFieldValue {
                            field_type: typ,
                            nullable,
                            value: value.to_string(),
                        })
                }
            }
            FieldType::Json => {
                if nullable && (value.is_empty() || value == "null") {
                    Ok(Field::Null)
                } else {
                    json_from_str(value).map(Field::Json).map_err(|_| {
                        TypeError::InvalidFieldValue {
                            field_type: typ,
                            nullable,
                            value: value.to_string(),
                        }
                    })
                }
            }
            FieldType::Point => {
                if nullable && (value.is_empty() || value == "null") {
                    Ok(Field::Null)
                } else {
                    value.parse::<DozerPoint>().map(Field::Point)
                }
            }
            FieldType::Duration => {
                if nullable && (value.is_empty() || value == "null") {
                    Ok(Field::Null)
                } else {
                    value.parse::<DozerDuration>().map(Field::Duration)
                }
            }
        }
    }
}

pub fn serialize_duration_secs_f64<S>(duration: &Option<Duration>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    duration.unwrap().as_secs_f64().serialize(s)
}

pub fn deserialize_duration_secs_f64<'a, D>(d: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'a>,
{
    f64::deserialize(d).map(|f| Some(Duration::from_secs_f64(f)))
}

pub fn f64_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    f64::json_schema(gen)
}

#[cfg(test)]
mod tests {
    use crate::json_types::json;
    use chrono::FixedOffset;
    use geo::Point;
    use rust_decimal::prelude::FromPrimitive;

    use super::*;

    #[test]
    fn test_field_from_str() {
        let ok_cases = [
            ("1", FieldType::UInt, false, Field::UInt(1)),
            ("1", FieldType::U128, false, Field::U128(1)),
            ("1", FieldType::Int, false, Field::Int(1)),
            ("1", FieldType::I128, false, Field::I128(1)),
            ("1.1", FieldType::Float, false, Field::Float(1.1.into())),
            ("true", FieldType::Boolean, false, Field::Boolean(true)),
            ("false", FieldType::Boolean, false, Field::Boolean(false)),
            (
                "hello",
                FieldType::String,
                false,
                Field::String("hello".to_string()),
            ),
            (
                "hello",
                FieldType::Text,
                false,
                Field::Text("hello".to_string()),
            ),
            (
                "1.1",
                FieldType::Decimal,
                false,
                Field::Decimal(Decimal::from_f64(1.1).unwrap()),
            ),
            (
                "2020-01-01T00:00:00Z",
                FieldType::Timestamp,
                false,
                Field::Timestamp(
                    "2020-01-01T00:00:00Z"
                        .parse::<DateTime<FixedOffset>>()
                        .unwrap(),
                ),
            ),
            (
                "2020-01-01",
                FieldType::Date,
                false,
                Field::Date("2020-01-01".parse::<NaiveDate>().unwrap()),
            ),
            (
                "(1,1)",
                FieldType::Point,
                false,
                Field::Point(DozerPoint(Point::new(OrderedFloat(1.0), OrderedFloat(1.0)))),
            ),
            (
                "{\"abc\":\"foo\"}",
                FieldType::Json,
                false,
                Field::Json(json!({"abc": "foo"})),
            ),
            ("null", FieldType::UInt, true, Field::Null),
            ("null", FieldType::U128, true, Field::Null),
            ("null", FieldType::Int, true, Field::Null),
            ("null", FieldType::I128, true, Field::Null),
            ("null", FieldType::Float, true, Field::Null),
            ("null", FieldType::Boolean, true, Field::Null),
            (
                "null",
                FieldType::String,
                true,
                Field::String("null".to_string()),
            ),
            (
                "null",
                FieldType::Text,
                true,
                Field::Text("null".to_string()),
            ),
            ("null", FieldType::Binary, true, Field::Null),
            ("null", FieldType::Decimal, true, Field::Null),
            ("null", FieldType::Timestamp, true, Field::Null),
            ("null", FieldType::Date, true, Field::Null),
            ("null", FieldType::Json, true, Field::Null),
            ("null", FieldType::Point, true, Field::Null),
            ("null", FieldType::Json, true, Field::Null),
            ("null", FieldType::Duration, true, Field::Null),
            ("", FieldType::UInt, true, Field::Null),
            ("", FieldType::U128, true, Field::Null),
            ("", FieldType::Int, true, Field::Null),
            ("", FieldType::I128, true, Field::Null),
            ("", FieldType::Float, true, Field::Null),
            ("", FieldType::Boolean, true, Field::Null),
            ("", FieldType::String, true, Field::String(String::new())),
            ("", FieldType::Text, true, Field::Text(String::new())),
            ("", FieldType::Binary, true, Field::Null),
            ("", FieldType::Decimal, true, Field::Null),
            ("", FieldType::Timestamp, true, Field::Null),
            ("", FieldType::Date, true, Field::Null),
            ("", FieldType::Json, true, Field::Null),
            ("", FieldType::Point, true, Field::Null),
            ("", FieldType::Duration, true, Field::Null),
        ];

        for case in ok_cases {
            assert_eq!(Field::from_str(case.0, case.1, case.2).unwrap(), case.3);
        }

        let err_cases = [
            ("null", FieldType::UInt, false),
            ("null", FieldType::U128, false),
            ("null", FieldType::Int, false),
            ("null", FieldType::I128, false),
            ("null", FieldType::Float, false),
            ("null", FieldType::Boolean, false),
            ("null", FieldType::Binary, false),
            ("null", FieldType::Decimal, false),
            ("null", FieldType::Timestamp, false),
            ("null", FieldType::Date, false),
            ("null", FieldType::Point, false),
            ("null", FieldType::Duration, false),
            ("", FieldType::UInt, false),
            ("", FieldType::U128, false),
            ("", FieldType::Int, false),
            ("", FieldType::I128, false),
            ("", FieldType::Float, false),
            ("", FieldType::Boolean, false),
            ("", FieldType::Binary, false),
            ("", FieldType::Decimal, false),
            ("", FieldType::Timestamp, false),
            ("", FieldType::Date, false),
            ("", FieldType::Point, false),
            ("", FieldType::Duration, false),
        ];
        for err_case in err_cases {
            assert!(Field::from_str(err_case.0, err_case.1, err_case.2).is_err());
        }
    }
}
