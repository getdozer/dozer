use crate::errors::types::{DeserializationError, TypeError};
use crate::json_types::{JsonValue, serde_json_to_json_value};
use crate::types::{DozerDuration, DozerPoint, TimeUnit, DATE_FORMAT};
use crate::types::{Field, FieldType};
use chrono::{DateTime, NaiveDate};
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;
use std::time::Duration;

/// Used in REST APIs and query expressions for converting JSON value to `Field`
pub fn json_value_to_field(
    value: Value,
    typ: FieldType,
    nullable: bool,
) -> Result<Field, TypeError> {
    if nullable {
        if let Value::Null = value {
            return Ok(Field::Null);
        }
    }

    match typ {
        FieldType::UInt => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::UInt),
        FieldType::U128 => match value {
            Value::String(str) => return Field::from_str(str.as_str(), typ, nullable),
            _ => Err(DeserializationError::Custom(
                "Json value type does not match field type"
                    .to_string()
                    .into(),
            )),
        },
        FieldType::Int => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::Int),
        FieldType::I128 => match value {
            Value::String(str) => return Field::from_str(str.as_str(), typ, nullable),
            _ => Err(DeserializationError::Custom(
                "Json value type does not match field type"
                    .to_string()
                    .into(),
            )),
        },
        FieldType::Float => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::Float),
        FieldType::Boolean => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::Boolean),
        FieldType::String => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::String),
        FieldType::Text => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::Text),
        FieldType::Binary => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::Binary),
        FieldType::Decimal => match value {
            Value::String(str) => return Field::from_str(str.as_str(), typ, nullable),
            Value::Number(number) => return Field::from_str(&number.to_string(), typ, nullable),
            _ => Err(DeserializationError::Custom(
                "Json value type does not match field type"
                    .to_string()
                    .into(),
            )),
        },
        FieldType::Timestamp => match value {
            Value::String(str) => return Field::from_str(str.as_str(), typ, nullable),
            _ => Err(DeserializationError::Custom(
                "Json value type does not match field type"
                    .to_string()
                    .into(),
            )),
        },
        FieldType::Date => match value {
            Value::String(str) => return Field::from_str(str.as_str(), typ, nullable),
            _ => Err(DeserializationError::Custom(
                "Json value type does not match field type"
                    .to_string()
                    .into(),
            )),
        },
        FieldType::Json => Ok(Field::Json(
            serde_json_to_json_value(value).map_err(TypeError::DeserializationError)?,
        )),
        FieldType::Point => serde_json::from_value(value)
            .map_err(DeserializationError::Json)
            .map(Field::Point),
        FieldType::Duration => match value.get("value") {
            Some(Value::String(v_val)) => match value.get("time_unit") {
                Some(Value::String(tu_val)) => {
                    let time_unit = TimeUnit::from_str(tu_val)?;
                    return match time_unit {
                        TimeUnit::Seconds => {
                            let dur = u64::from_str(v_val).unwrap();
                            Ok(Field::Duration(DozerDuration(
                                Duration::from_secs(dur),
                                time_unit,
                            )))
                        }
                        TimeUnit::Milliseconds => {
                            let dur = u64::from_str(v_val).unwrap();
                            Ok(Field::Duration(DozerDuration(
                                Duration::from_millis(dur),
                                time_unit,
                            )))
                        }
                        TimeUnit::Microseconds => {
                            let dur = u64::from_str(v_val).unwrap();
                            Ok(Field::Duration(DozerDuration(
                                Duration::from_micros(dur),
                                time_unit,
                            )))
                        }
                        TimeUnit::Nanoseconds => {
                            Ok(Field::Duration(DozerDuration::from_str(v_val).unwrap()))
                        }
                    };
                }
                _ => Err(DeserializationError::Custom(
                    "Json value type does not match field type"
                        .to_string()
                        .into(),
                )),
            },
            _ => Err(DeserializationError::Custom(
                "Json value type does not match field type"
                    .to_string()
                    .into(),
            )),
        },
    }
    .map_err(TypeError::DeserializationError)
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
                    JsonValue::from_str(value)
                        .map(Field::Json)
                        .map_err(|_| TypeError::InvalidFieldValue {
                            field_type: typ,
                            nullable,
                            value: value.to_string(),
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

#[cfg(test)]
mod tests {
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
            ("null", FieldType::Json, false),
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
            ("", FieldType::Json, false),
            ("", FieldType::Point, false),
            ("", FieldType::Duration, false),
        ];
        for err_case in err_cases {
            assert!(Field::from_str(err_case.0, err_case.1, err_case.2).is_err());
        }
    }
}
