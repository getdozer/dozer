use chrono::SecondsFormat;
use ordered_float::OrderedFloat;
use serde_json::{Map, Value};

use crate::types::{DozerDuration, Field, DATE_FORMAT};

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
        Field::Json(b) => Value::from(b),
        Field::Point(point) => convert_x_y_to_object(&point.0.x_y()),
        Field::Duration(d) => convert_duration_to_object(&d),
        Field::Null => Value::Null,
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
    use serde_json::json;

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
                Field::Json(json!([
                    123, 34, 97, 98, 99, 34, 58, 34, 102, 111, 111, 34, 125,
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
