use dozer_cache::cache::RecordWithId as CacheRecordWithId;
use dozer_types::chrono::SecondsFormat;
use dozer_types::types::{
    Field, FieldType, Operation as DozerOperation, Record as DozerRecord, DATE_FORMAT,
};

use crate::grpc::types::{value, Operation, OperationType, Record, Type, Value};

use super::types::RecordWithId;

pub fn map_operation(endpoint_name: String, operation: &DozerOperation) -> Operation {
    match operation.to_owned() {
        DozerOperation::Delete { old } => Operation {
            typ: OperationType::Delete as i32,
            old: Some(record_to_internal_record(old)),
            new: None,
            endpoint_name,
        },
        DozerOperation::Insert { new } => Operation {
            typ: OperationType::Insert as i32,
            old: None,
            new: Some(record_to_internal_record(new)),
            endpoint_name,
        },
        DozerOperation::Update { old, new } => Operation {
            typ: OperationType::Insert as i32,
            old: Some(record_to_internal_record(old)),
            new: Some(record_to_internal_record(new)),
            endpoint_name,
        },
    }
}

fn record_to_internal_record(record: DozerRecord) -> Record {
    let values: Vec<Value> = record
        .values
        .into_iter()
        .map(field_to_prost_value)
        .collect();

    Record {
        values,
        version: record.version,
    }
}

pub fn map_record(record: CacheRecordWithId) -> RecordWithId {
    RecordWithId {
        id: record.id,
        record: Some(record_to_internal_record(record.record)),
    }
}

pub fn field_to_prost_value(f: Field) -> Value {
    match f {
        Field::UInt(n) => Value {
            value: Some(value::Value::UintValue(n)),
        },
        Field::Int(n) => Value {
            value: Some(value::Value::IntValue(n)),
        },
        Field::Float(n) => Value {
            value: Some(value::Value::DoubleValue(n.0)),
        },

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
        Field::Decimal(n) => Value {
            value: Some(value::Value::StringValue(n.to_string())),
        },
        Field::Timestamp(ts) => Value {
            value: Some(value::Value::StringValue(
                ts.to_rfc3339_opts(SecondsFormat::Millis, true),
            )),
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
    }
}

pub fn map_field_definitions(
    fields: Vec<dozer_types::types::FieldDefinition>,
) -> Vec<crate::grpc::types::FieldDefinition> {
    fields
        .into_iter()
        .map(|f| crate::grpc::types::FieldDefinition {
            typ: field_type_to_internal_type(f.typ) as i32,
            name: f.name,
            nullable: f.nullable,
        })
        .collect()
}

fn field_type_to_internal_type(typ: FieldType) -> Type {
    match typ {
        FieldType::UInt => Type::UInt,
        FieldType::Int => Type::Int,
        FieldType::Float => Type::Float,
        FieldType::Boolean => Type::Boolean,
        FieldType::String => Type::String,
        FieldType::Text => Type::Text,
        FieldType::Binary => Type::Binary,
        FieldType::Decimal => Type::Decimal,
        FieldType::Timestamp => Type::Timestamp,
        FieldType::Bson => Type::Bson,
        FieldType::Date => Type::String,
    }
}
