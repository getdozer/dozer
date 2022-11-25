use dozer_types::chrono::SecondsFormat;
use dozer_types::types::{
    Field, FieldType, Operation as DozerOperation, Record as DozerRecord, Schema as DozerSchema,
    DATE_FORMAT,
};

use crate::grpc::types::{
    value, FieldDefinition, Operation, OperationType, Record, SchemaEvent, Type, Value,
};

pub fn map_schema(endpoint_name: String, schema: &DozerSchema) -> SchemaEvent {
    let fields = schema
        .fields
        .iter()
        .map(|f| FieldDefinition {
            typ: map_field_type_to_pb(&f.typ) as i32,
            name: f.name.to_owned(),
            nullable: f.nullable,
        })
        .collect();
    let primary_index = schema.primary_index.iter().map(|f| *f as i32).collect();
    SchemaEvent {
        endpoint: endpoint_name,
        version: schema.identifier.as_ref().unwrap().version as u64,
        fields,
        primary_index,
    }
}

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
    let values: Vec<Value> = record.values.iter().map(field_to_prost_value).collect();

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
        Field::Date(date) => Value {
            value: Some(value::Value::StringValue(
                date.format(DATE_FORMAT).to_string(),
            )),
        },
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
        FieldType::Decimal => Type::Decimal,
        FieldType::Timestamp => Type::Timestamp,
        FieldType::Bson => Type::Bson,
        FieldType::Null => Type::Null,
        FieldType::Date => Type::String,
    }
}
