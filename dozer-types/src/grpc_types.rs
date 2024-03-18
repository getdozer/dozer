pub mod types {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.types"); // The string specified here must match the proto package name
}

pub mod common {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.common"); // The string specified here must match the proto package name
}
pub mod health {
    #![allow(non_camel_case_types)]
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.health"); // The string specified here must match the proto package name
}
pub mod internal {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.internal");
}

pub mod auth {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.auth");
}

pub mod ingest {
    tonic::include_proto!("dozer.ingest");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("ingest");
}

pub mod cloud {
    #![allow(clippy::derive_partial_eq_without_eq, clippy::large_enum_variant)]
    tonic::include_proto!("dozer.cloud");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("cloud");
    use crate::chrono::NaiveDateTime;
    use prost_types::Timestamp;
    pub fn naive_datetime_to_timestamp(naive_dt: NaiveDateTime) -> Timestamp {
        let unix_timestamp = naive_dt.timestamp(); // Get the UNIX timestamp (seconds since epoch)
        let nanos = naive_dt.timestamp_subsec_nanos() as i32; // Get nanoseconds part
        prost_types::Timestamp {
            seconds: unix_timestamp,
            nanos,
        }
    }
}

pub mod contract {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.contract");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("contract");
}

pub mod app_ui {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.app_ui");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("app_ui");
}

pub mod telemetry {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.telemetry");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("telemetry");
}

pub mod api_explorer {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.api_explorer");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("api_explorer");
}

// To be used in tests
pub mod generated {
    pub mod films {
        #![allow(clippy::derive_partial_eq_without_eq)]
        #![allow(non_camel_case_types)]
        tonic::include_proto!("dozer.generated.films");
    }
}

pub mod conversions {
    use super::types::{value, DurationType, PointType, RustDecimal, Type, Value};
    use crate::json_types::json_value_to_prost;
    use crate::ordered_float::OrderedFloat;
    use crate::rust_decimal::Decimal;
    use crate::types::{DozerDuration, Field, FieldType, DATE_FORMAT};
    use prost_types::Timestamp;

    fn map_x_y_to_prost_coord_map((x, y): (OrderedFloat<f64>, OrderedFloat<f64>)) -> Value {
        Value {
            value: Some(value::Value::PointValue(PointType { x: x.0, y: y.0 })),
        }
    }

    fn map_duration_to_prost_coord_map(d: DozerDuration) -> Value {
        Value {
            value: Some(value::Value::DurationValue(DurationType {
                value: d.0.as_nanos().to_string(),
                time_unit: d.1.to_string(),
            })),
        }
    }

    fn map_decimal(d: Decimal) -> Value {
        Value {
            value: Some(value::Value::DecimalValue(RustDecimal {
                scale: d.unpack().scale,
                lo: d.unpack().lo,
                mid: d.unpack().mid,
                hi: d.unpack().hi,
                negative: d.unpack().negative,
            })),
        }
    }

    pub fn field_to_grpc(f: Field) -> Value {
        match f {
            Field::UInt(n) => Value {
                value: Some(value::Value::UintValue(n)),
            },
            Field::U128(n) => Value {
                value: Some(value::Value::Uint128Value(n.to_string())),
            },
            Field::Int(n) => Value {
                value: Some(value::Value::IntValue(n)),
            },
            Field::I128(n) => Value {
                value: Some(value::Value::Int128Value(n.to_string())),
            },
            Field::Float(n) => Value {
                value: Some(value::Value::FloatValue(n.0)),
            },
            Field::Decimal(d) => map_decimal(d),
            Field::Boolean(n) => Value {
                value: Some(value::Value::BoolValue(n)),
            },
            Field::String(s) => Value {
                value: Some(value::Value::StringValue(s)),
            },
            Field::Text(s) => Value {
                value: Some(value::Value::StringValue(s)),
            },
            Field::Binary(b) => Value {
                value: Some(value::Value::BytesValue(b)),
            },
            Field::Timestamp(ts) => Value {
                value: Some(value::Value::TimestampValue(Timestamp {
                    seconds: ts.timestamp(),
                    nanos: ts.timestamp_subsec_nanos() as i32,
                })),
            },
            Field::Json(b) => Value {
                value: Some(value::Value::JsonValue(json_value_to_prost(b))),
            },
            Field::Null => Value { value: None },
            Field::Date(date) => Value {
                value: Some(value::Value::StringValue(
                    date.format(DATE_FORMAT).to_string(),
                )),
            },
            Field::Point(point) => map_x_y_to_prost_coord_map(point.0.x_y()),
            Field::Duration(d) => map_duration_to_prost_coord_map(d),
        }
    }

    pub fn field_definition_to_grpc(
        fields: Vec<crate::types::FieldDefinition>,
    ) -> Vec<super::types::FieldDefinition> {
        fields
            .into_iter()
            .map(|f| super::types::FieldDefinition {
                typ: field_type_to_internal_type(f.typ) as i32,
                name: f.name,
                nullable: f.nullable,
            })
            .collect()
    }

    fn field_type_to_internal_type(typ: FieldType) -> Type {
        match typ {
            FieldType::UInt => Type::UInt,
            FieldType::U128 => Type::U128,
            FieldType::Int => Type::Int,
            FieldType::I128 => Type::I128,
            FieldType::Float => Type::Float,
            FieldType::Boolean => Type::Boolean,
            FieldType::String => Type::String,
            FieldType::Text => Type::Text,
            FieldType::Binary => Type::Binary,
            FieldType::Decimal => Type::Decimal,
            FieldType::Timestamp => Type::Timestamp,
            FieldType::Json => Type::Json,
            FieldType::Date => Type::String,
            FieldType::Point => Type::Point,
            FieldType::Duration => Type::Duration,
        }
    }
    pub fn map_schema(schema: crate::types::Schema) -> crate::grpc_types::types::Schema {
        crate::grpc_types::types::Schema {
            primary_index: schema.primary_index.into_iter().map(|i| i as i32).collect(),
            fields: field_definition_to_grpc(schema.fields),
        }
    }
}
