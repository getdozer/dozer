use crate::grpc::types::{self as GrpcTypes};
use dozer_types::types::{Field, Record};
use inflector::Inflector;
use prost_reflect::{DescriptorPool, MessageDescriptor};
use prost_reflect::{DynamicMessage, Value};

use super::TypedResponse;

pub fn get_response_descriptor(
    desc: &DescriptorPool,
    method: &str,
    endpoint_name: &str,
) -> MessageDescriptor {
    match method {
        "query" => {
            let query_path = format!(
                "dozer.generated.{}.Query{}Response",
                endpoint_name.to_lowercase().to_plural(),
                endpoint_name.to_pascal_case().to_plural(),
            );

            desc.get_message_by_name(&query_path)
                .unwrap_or_else(|| panic!("{}: not found", query_path))
        }
        "on_event" => {
            let query_path = format!(
                "dozer.generated.{}.{}Event",
                endpoint_name.to_lowercase().to_plural(),
                endpoint_name.to_pascal_case().to_singular(),
            );

            desc.get_message_by_name(&query_path)
                .unwrap_or_else(|| panic!("{}: not found", query_path))
        }
        _ => panic!("method not found"),
    }
}

pub fn get_resource_desc(desc: &DescriptorPool, endpoint_name: &str) -> MessageDescriptor {
    let msg_path = format!(
        "dozer.generated.{}.{}",
        endpoint_name.to_lowercase().to_plural(),
        endpoint_name.to_pascal_case().to_singular(),
    );

    desc.get_message_by_name(&msg_path)
        .unwrap_or_else(|| panic!("{}: not found", msg_path))
}

pub fn on_event_to_typed_response(
    op: GrpcTypes::Operation,
    desc: &DescriptorPool,
    endpoint_name: &str,
) -> TypedResponse {
    let event_desc = get_response_descriptor(desc, "on_event", endpoint_name);

    let mut event = DynamicMessage::new(event_desc);
    event.set_field_by_name("typ", prost_reflect::Value::EnumNumber(op.typ));
    if let Some(old) = op.old {
        event.set_field_by_name(
            "old",
            prost_reflect::Value::Message(internal_record_to_pb(old, desc, endpoint_name)),
        );
    }

    if let Some(new) = op.new {
        event.set_field_by_name(
            "new",
            prost_reflect::Value::Message(internal_record_to_pb(new, desc, endpoint_name)),
        );
    }

    TypedResponse::new(event)
}

pub fn internal_record_to_pb(
    rec: GrpcTypes::Record,
    desc: &DescriptorPool,
    endpoint_name: &str,
) -> DynamicMessage {
    let msg_path = format!(
        "dozer.generated.{}.{}",
        endpoint_name.to_lowercase().to_plural(),
        endpoint_name.to_pascal_case().to_singular(),
    );
    let resource_desc = desc
        .get_message_by_name(&msg_path)
        .unwrap_or_else(|| panic!("{}: not found", msg_path));
    let mut resource = DynamicMessage::new(resource_desc.to_owned());

    for (field, value) in resource_desc.fields().zip(rec.values.into_iter()) {
        if let Some(value) = value.value {
            let value = convert_internal_type_to_pb(value);
            resource.set_field(&field, value);
        }
    }
    resource
}

fn convert_internal_type_to_pb(value: GrpcTypes::value::Value) -> prost_reflect::Value {
    match value {
        GrpcTypes::value::Value::UintValue(n) => Value::U64(n),
        GrpcTypes::value::Value::IntValue(n) => Value::I64(n),
        GrpcTypes::value::Value::FloatValue(n) => Value::F32(n),
        GrpcTypes::value::Value::BoolValue(n) => Value::Bool(n),
        GrpcTypes::value::Value::StringValue(n) => Value::String(n),
        GrpcTypes::value::Value::BytesValue(n) => {
            Value::Bytes(prost_reflect::bytes::Bytes::from(n.to_vec()))
        }
        GrpcTypes::value::Value::DoubleValue(n) => Value::F64(n),
        _ => todo!(),
    }
}
pub fn record_to_pb(record: Record, desc: &MessageDescriptor) -> DynamicMessage {
    let mut resource = DynamicMessage::new(desc.clone());
    for (field, value) in desc.fields().zip(record.values.into_iter()) {
        if let Field::Null = value {
            // Don't set the field if null
        } else {
            resource.set_field(&field, convert_field_to_reflect_value(value));
        }
    }
    resource
}

pub fn query_response_to_typed_response(
    records: Vec<Record>,
    desc: &DescriptorPool,
    endpoint_name: &str,
) -> TypedResponse {
    let query_desc = get_response_descriptor(desc, "query", endpoint_name);

    let mut msg = DynamicMessage::new(query_desc);

    let resource_desc = get_resource_desc(desc, endpoint_name);
    let resources = records
        .into_iter()
        .map(|rec| prost_reflect::Value::Message(record_to_pb(rec, &resource_desc)))
        .collect::<Vec<_>>();
    msg.set_field_by_name("data", prost_reflect::Value::List(resources));
    TypedResponse::new(msg)
}

fn convert_field_to_reflect_value(field: Field) -> prost_reflect::Value {
    match field {
        Field::UInt(n) => Value::U64(n),
        Field::Int(n) => Value::I64(n),
        Field::Float(n) => Value::F64(n.0),
        Field::Boolean(n) => Value::Bool(n),
        Field::String(n) => Value::String(n),
        Field::Text(n) => Value::String(n),
        Field::Binary(n) => Value::Bytes(prost_reflect::bytes::Bytes::from(n)),
        Field::Decimal(n) => Value::String(n.to_string()),
        Field::Timestamp(n) => Value::String(n.to_rfc3339()),
        Field::Date(n) => Value::String(n.to_string()),
        Field::Bson(n) => Value::Bytes(prost_reflect::bytes::Bytes::from(n)),
        Field::Null => panic!(),
    }
}
