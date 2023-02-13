use crate::grpc::types::{self as GrpcTypes};
use crate::grpc::types_helper::field_to_prost_value;
use dozer_cache::cache::RecordWithId;
use dozer_types::types::Record;
use inflector::Inflector;
use prost_reflect::{DescriptorPool, MessageDescriptor};
use prost_reflect::{DynamicMessage, Value};

use super::TypedResponse;

fn get_response_descriptor(
    desc: &DescriptorPool,
    method: &str,
    endpoint_name: &str,
) -> MessageDescriptor {
    match method {
        "count" => {
            let count_path = format!(
                "dozer.generated.{}.Count{}Response",
                endpoint_name.to_lowercase(),
                endpoint_name.to_pascal_case().to_plural(),
            );

            desc.get_message_by_name(&count_path)
                .unwrap_or_else(|| panic!("{count_path}: not found"))
        }
        "query" => {
            let query_path = format!(
                "dozer.generated.{}.Query{}Response",
                endpoint_name.to_lowercase(),
                endpoint_name.to_pascal_case().to_plural(),
            );

            desc.get_message_by_name(&query_path)
                .unwrap_or_else(|| panic!("{query_path}: not found"))
        }
        "on_event" => {
            let query_path = format!(
                "dozer.generated.{}.{}Event",
                endpoint_name.to_lowercase(),
                endpoint_name.to_pascal_case().to_singular(),
            );

            desc.get_message_by_name(&query_path)
                .unwrap_or_else(|| panic!("{query_path}: not found"))
        }
        "token" => {
            let token_path = format!(
                "dozer.generated.{}.TokenResponse",
                endpoint_name.to_lowercase(),
            );
            desc.get_message_by_name(&token_path)
                .unwrap_or_else(|| panic!("{token_path}: not found"))
        }
        _ => panic!("method not found"),
    }
}

fn get_resource_desc(desc: &DescriptorPool, endpoint_name: &str) -> MessageDescriptor {
    let msg_path = format!(
        "dozer.generated.{}.{}",
        endpoint_name.to_lowercase(),
        endpoint_name.to_pascal_case().to_singular(),
    );

    desc.get_message_by_name(&msg_path)
        .unwrap_or_else(|| panic!("{msg_path}: not found"))
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

fn internal_record_to_pb(
    rec: GrpcTypes::Record,
    desc: &DescriptorPool,
    endpoint_name: &str,
) -> DynamicMessage {
    let msg_path = format!(
        "dozer.generated.{}.{}",
        endpoint_name.to_lowercase(),
        endpoint_name.to_pascal_case().to_singular(),
    );
    let resource_desc = desc
        .get_message_by_name(&msg_path)
        .unwrap_or_else(|| panic!("{msg_path}: not found"));
    let mut resource = DynamicMessage::new(resource_desc.to_owned());

    for (field, value) in resource_desc.fields().zip(rec.values.into_iter()) {
        if let Some(value) = interval_value_to_pb(value) {
            resource.set_field(&field, value);
        }
    }
    resource
}

fn interval_value_to_pb(value: GrpcTypes::Value) -> Option<prost_reflect::Value> {
    value.value.map(|value| match value {
        GrpcTypes::value::Value::UintValue(n) => Value::U64(n),
        GrpcTypes::value::Value::IntValue(n) => Value::I64(n),
        GrpcTypes::value::Value::FloatValue(n) => Value::F32(n),
        GrpcTypes::value::Value::BoolValue(n) => Value::Bool(n),
        GrpcTypes::value::Value::StringValue(n) => Value::String(n),
        GrpcTypes::value::Value::BytesValue(n) => {
            Value::Bytes(prost_reflect::bytes::Bytes::from(n))
        }
        GrpcTypes::value::Value::DoubleValue(n) => Value::F64(n),
        _ => todo!(),
    })
}
fn record_to_pb(record: Record, desc: &MessageDescriptor) -> DynamicMessage {
    let mut resource = DynamicMessage::new(desc.clone());
    for (field, value) in desc.fields().zip(record.values.into_iter()) {
        if let Some(value) = interval_value_to_pb(field_to_prost_value(value)) {
            resource.set_field(&field, value);
        }
    }
    resource
}

pub fn count_response_to_typed_response(
    count: usize,
    desc: &DescriptorPool,
    endpoint_name: &str,
) -> TypedResponse {
    let count_desc = get_response_descriptor(desc, "count", endpoint_name);

    let mut msg = DynamicMessage::new(count_desc);
    msg.set_field_by_name("count", prost_reflect::Value::U64(count as _));

    TypedResponse::new(msg)
}

pub fn query_response_to_typed_response(
    records: Vec<RecordWithId>,
    desc: &DescriptorPool,
    endpoint_name: &str,
) -> TypedResponse {
    let query_desc = get_response_descriptor(desc, "query", endpoint_name);

    let mut msg = DynamicMessage::new(query_desc);

    let resource_desc = get_resource_desc(desc, endpoint_name);
    let resources = records
        .into_iter()
        .map(|rec| prost_reflect::Value::Message(record_to_pb(rec.record, &resource_desc)))
        .collect::<Vec<_>>();
    msg.set_field_by_name("data", prost_reflect::Value::List(resources));
    TypedResponse::new(msg)
}

pub fn token_response(token: String, desc: &DescriptorPool, endpoint_name: &str) -> TypedResponse {
    let token_desc = get_response_descriptor(desc, "token", endpoint_name);
    let mut msg = DynamicMessage::new(token_desc);
    msg.set_field_by_name("token", prost_reflect::Value::String(token));
    TypedResponse::new(msg)
}
