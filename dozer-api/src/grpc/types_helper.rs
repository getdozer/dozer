use dozer_cache::cache::CacheRecord;
use dozer_types::grpc_types::types::{
    value, DurationType, Operation, OperationType, PointType, Record, RecordWithId, RustDecimal,
    Type, Value,
};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{DozerDuration, Field, FieldType, DATE_FORMAT};
use prost_reflect::prost_types::Timestamp;

pub fn map_insert_operation(endpoint_name: String, record: CacheRecord) -> Operation {
    Operation {
        typ: OperationType::Insert as i32,
        old: None,
        new_id: Some(record.id),
        new: Some(record_to_internal_record(record)),
        endpoint_name,
    }
}

pub fn map_delete_operation(endpoint_name: String, record: CacheRecord) -> Operation {
    Operation {
        typ: OperationType::Delete as i32,
        old: None,
        new: Some(record_to_internal_record(record)),
        new_id: None,
        endpoint_name,
    }
}

pub fn map_update_operation(
    endpoint_name: String,
    old: CacheRecord,
    new: CacheRecord,
) -> Operation {
    Operation {
        typ: OperationType::Update as i32,
        old: Some(record_to_internal_record(old)),
        new: Some(record_to_internal_record(new)),
        new_id: None,
        endpoint_name,
    }
}

fn record_to_internal_record(record: CacheRecord) -> Record {
    let values: Vec<Value> = record
        .record
        .values
        .into_iter()
        .map(field_to_prost_value)
        .collect();

    Record {
        values,
        version: record.version,
    }
}

pub fn map_record(record: CacheRecord) -> RecordWithId {
    RecordWithId {
        id: record.id,
        record: Some(record_to_internal_record(record)),
    }
}

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
            flags: d.unpack().negative as u32,
            lo: d.unpack().lo,
            mid: d.unpack().mid,
            hi: d.unpack().hi,
        })),
    }
}

fn field_to_prost_value(f: Field) -> Value {
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
        Field::Bson(b) => Value {
            value: Some(value::Value::BytesValue(b)),
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

pub fn map_field_definitions(
    fields: Vec<dozer_types::types::FieldDefinition>,
) -> Vec<dozer_types::grpc_types::types::FieldDefinition> {
    fields
        .into_iter()
        .map(|f| dozer_types::grpc_types::types::FieldDefinition {
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
        FieldType::Bson => Type::Bson,
        FieldType::Date => Type::String,
        FieldType::Point => Type::Point,
        FieldType::Duration => Type::Duration,
    }
}
