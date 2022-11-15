use crate::grpc::services::common::common_grpc::{value, ArrayValue, Type, Value};
use dozer_types::chrono::SecondsFormat;
use dozer_types::types::{Field, FieldType, Operation as DozerOperation, Record as DozerRecord};

use super::common_grpc::{Operation, OperationType, Record};

pub fn map_operation(endpoint_name: String, operation: &DozerOperation) -> Operation {
    match operation.to_owned() {
        DozerOperation::Delete { old } => Operation {
            typ: OperationType::Delete as i32,
            old: Some(map_record(old)),
            new: None,
            endpoint_name,
        },
        DozerOperation::Insert { new } => Operation {
            typ: OperationType::Insert as i32,
            old: None,
            new: Some(map_record(new)),
            endpoint_name,
        },
        DozerOperation::Update { old, new } => Operation {
            typ: OperationType::Insert as i32,
            old: Some(map_record(old)),
            new: Some(map_record(new)),
            endpoint_name,
        },
    }
}

pub fn map_record(record: DozerRecord) -> Record {
    let values: Vec<Value> = record
        .to_owned()
        .values
        .iter()
        .map(field_to_prost_value)
        .collect();

    Record { values }
}
pub fn field_to_prost_value(f: &Field) -> Value {
    match f {
        Field::UInt(n) => Value {
            value: Some(value::Value::UintValue(*n)),
        },
        Field::Int(n) => Value {
            value: Some(value::Value::IntValue(*n)),
        },
        Field::Float(n) => Value {
            value: Some(value::Value::FloatValue(n.0 as f32)),
        },

        Field::Boolean(n) => Value {
            value: Some(value::Value::BoolValue(*n)),
        },

        Field::String(s) => Value {
            value: Some(value::Value::StringValue(s.to_owned())),
        },
        Field::Text(s) => Value {
            value: Some(value::Value::StringValue(s.to_owned())),
        },
        Field::Binary(b) => Value {
            value: Some(value::Value::BytesValue(b.to_owned())),
        },
        Field::UIntArray(arr) => Value {
            value: Some(value::Value::ArrayValue(ArrayValue {
                array_value: arr
                    .iter()
                    .map(|v| Value {
                        value: Some(value::Value::UintValue(*v)),
                    })
                    .collect(),
            })),
        },
        Field::IntArray(arr) => Value {
            value: Some(value::Value::ArrayValue(ArrayValue {
                array_value: arr
                    .iter()
                    .map(|v| Value {
                        value: Some(value::Value::IntValue(*v)),
                    })
                    .collect(),
            })),
        },
        Field::FloatArray(arr) => Value {
            value: Some(value::Value::ArrayValue(ArrayValue {
                array_value: arr
                    .iter()
                    .map(|v| Value {
                        value: Some(value::Value::FloatValue(v.0 as f32)),
                    })
                    .collect(),
            })),
        },
        Field::BooleanArray(arr) => Value {
            value: Some(value::Value::ArrayValue(ArrayValue {
                array_value: arr
                    .iter()
                    .map(|v| Value {
                        value: Some(value::Value::BoolValue(*v)),
                    })
                    .collect(),
            })),
        },
        Field::StringArray(arr) => Value {
            value: Some(value::Value::ArrayValue(ArrayValue {
                array_value: arr
                    .iter()
                    .map(|v| Value {
                        value: Some(value::Value::StringValue(v.to_owned())),
                    })
                    .collect(),
            })),
        },
        Field::Decimal(n) => Value {
            value: Some(value::Value::StringValue((*n).to_string())),
        },
        Field::Timestamp(ts) => Value {
            value: Some(value::Value::StringValue(
                ts.to_rfc3339_opts(SecondsFormat::Millis, true),
            )),
        },

        Field::Bson(b) => Value {
            value: Some(value::Value::BytesValue(b.to_owned())),
        },
        Field::Null => Value { value: None },
    }
}

pub fn map_field_type_to_pb(typ: &FieldType) -> Type {
    match typ {
        FieldType::UInt => Type::UInt,
        FieldType::Int => Type::Int,
        FieldType::Float => Type::Float,
        FieldType::Boolean => Type::Boolean,
        FieldType::String => Type::String,
        FieldType::Text => Type::Text,
        FieldType::Binary => Type::Binary,
        FieldType::UIntArray => Type::UIntArray,
        FieldType::IntArray => Type::IntArray,
        FieldType::FloatArray => Type::FloatArray,
        FieldType::BooleanArray => Type::BooleanArray,
        FieldType::StringArray => Type::StringArray,
        FieldType::Decimal => Type::Decimal,
        FieldType::Timestamp => Type::Timestamp,
        FieldType::Bson => Type::Bson,
        FieldType::Null => Type::Null,
    }
}
