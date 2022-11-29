use crate::grpc::types::{self as GrpcTypes, SchemaEvent};
use dozer_cache::{cache::expression::QueryExpression, errors::CacheError};
use dozer_types::serde_json;
use dozer_types::types::{Field, Record, Schema};
use inflector::Inflector;
use prost_reflect::{DescriptorPool, MessageDescriptor, SerializeOptions};
use prost_reflect::{DynamicMessage, Value};

use tonic::{Code, Status};

use super::TypedResponse;

pub fn get_response_descriptor(
    desc: DescriptorPool,
    method: &str,
    endpoint_name: String,
) -> MessageDescriptor {
    match method {
        "query" => {
            let query_path = format!(
                "dozer.generated.{}.Query{}Response",
                endpoint_name.to_lowercase().to_plural(),
                endpoint_name.to_pascal_case().to_plural(),
            );
            let msg_desc = desc
                .get_message_by_name(&query_path)
                .expect(&format!("{}: not found", query_path));
            msg_desc
        }
        "on_event" => {
            let query_path = format!(
                "dozer.generated.{}.{}Event",
                endpoint_name.to_lowercase().to_plural(),
                endpoint_name.to_pascal_case().to_singular(),
            );
            let msg_desc = desc
                .get_message_by_name(&query_path)
                .expect(&format!("{}: not found", query_path));
            msg_desc
        }
        _ => panic!("method not found"),
    }
}

pub fn get_resource_desc(desc: DescriptorPool, endpoint_name: String) -> MessageDescriptor {
    let msg_path = format!(
        "dozer.generated.{}.{}",
        endpoint_name.to_lowercase().to_plural(),
        endpoint_name.to_pascal_case().to_singular(),
    );
    let resource_desc = desc
        .get_message_by_name(&msg_path)
        .expect(&format!("{}: not found", msg_path));
    resource_desc
}

pub fn on_event_to_typed_response(
    op: GrpcTypes::Operation,
    schema_event: SchemaEvent,
    desc: DescriptorPool,
    endpoint_name: String,
) -> TypedResponse {
    let event_desc = get_response_descriptor(desc.to_owned(), "on_event", endpoint_name.to_owned());

    let mut event = DynamicMessage::new(event_desc.to_owned());
    event.set_field_by_name("typ", prost_reflect::Value::EnumNumber(op.typ));
    if let Some(old) = op.old {
        event.set_field_by_name(
            "old",
            prost_reflect::Value::Message(internal_record_to_pb(
                old,
                schema_event.clone(),
                desc.clone(),
                endpoint_name.clone(),
            )),
        );
    }

    if let Some(new) = op.new {
        event.set_field_by_name(
            "new",
            prost_reflect::Value::Message(internal_record_to_pb(
                new,
                schema_event,
                desc,
                endpoint_name,
            )),
        );
    }

    TypedResponse::new(event)
}

pub fn internal_record_to_pb(
    rec: GrpcTypes::Record,
    schema_event: SchemaEvent,
    desc: DescriptorPool,
    endpoint_name: String,
) -> DynamicMessage {
    let msg_path = format!(
        "dozer.generated.{}.{}",
        endpoint_name.to_lowercase().to_plural(),
        endpoint_name.to_pascal_case().to_singular(),
    );
    let resource_desc = desc
        .get_message_by_name(&msg_path)
        .expect(&format!("{}: not found", msg_path));
    let mut resource = DynamicMessage::new(resource_desc.to_owned());

    for fd in resource_desc.fields() {
        let (idx, _) = schema_event
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == fd.name())
            .expect("field to be present");
        let field = rec.values.get(idx).expect("field to be present in record");

        if let Some(value) = field.value.clone() {
            let val = convert_internal_type_to_pb(value);
            resource.set_field(&fd, val);
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
pub fn record_to_pb(
    rec: Record,
    schema: Schema,
    desc: DescriptorPool,
    endpoint_name: String,
) -> DynamicMessage {
    let resource_desc = get_resource_desc(desc, endpoint_name);
    let mut resource = DynamicMessage::new(resource_desc.to_owned());

    for fd in resource_desc.fields() {
        let (idx, _) = schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == fd.name())
            .expect("field to be present");
        let field = rec.values.get(idx).expect("field to be present in record");

        if let Field::Null = field {
            // Don't set the field if null
        } else {
            resource.set_field(&fd, convert_field_to_reflect_value(field));
        }
    }
    resource
}

pub fn query_response_to_typed_response(
    records: Vec<Record>,
    schema: Schema,
    desc: DescriptorPool,
    endpoint_name: String,
) -> TypedResponse {
    let query_desc = get_response_descriptor(desc.to_owned(), "query", endpoint_name.to_owned());

    let mut msg = DynamicMessage::new(query_desc.to_owned());

    let mut resources = vec![];
    for record in records {
        let resource = record_to_pb(record, schema.clone(), desc.clone(), endpoint_name.clone());
        resources.push(prost_reflect::Value::Message(resource));
    }
    msg.set_field_by_name(
        &endpoint_name.to_plural(),
        prost_reflect::Value::List(resources),
    );
    TypedResponse::new(msg)
}

fn convert_field_to_reflect_value(field: &Field) -> prost_reflect::Value {
    match field {
        Field::UInt(n) => Value::U64(*n),
        Field::Int(n) => Value::I64(*n),
        Field::Float(n) => todo!(),
        Field::Boolean(n) => Value::Bool(*n),
        Field::String(n) => Value::String(n.clone()),
        Field::Text(n) => Value::String(n.clone()),
        Field::Binary(n) => Value::Bytes(prost_reflect::bytes::Bytes::from(n.to_vec())),
        Field::Decimal(n) => Value::String(n.to_string()),
        Field::Timestamp(n) => Value::String(n.to_rfc3339()),
        Field::Date(n) => Value::String(n.to_string()),
        Field::Bson(n) => Value::Bytes(prost_reflect::bytes::Bytes::from(n.to_vec())),
        Field::Null => todo!(),
    }
}

pub fn from_dynamic_message_to_json(input: DynamicMessage) -> Result<serde_json::Value, Status> {
    let mut options = SerializeOptions::new();
    options = options.use_proto_field_name(true);
    let mut serializer = serde_json::Serializer::new(vec![]);
    input
        .serialize_with_options(&mut serializer, &options)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    let string_utf8 = String::from_utf8(serializer.into_inner())
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    let result: serde_json::Value = serde_json::from_str(&string_utf8)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    Ok(result)
}

pub fn from_cache_error(error: CacheError) -> Status {
    Status::new(Code::Internal, error.to_string())
}

pub fn convert_grpc_message_to_query_exp(input: DynamicMessage) -> Result<QueryExpression, Status> {
    let json_present = from_dynamic_message_to_json(input)?;
    let mut string_present = json_present.to_string();
    let key_replace = vec![
        "filter", "and", "limit", "skip", "order_by", "eq", "lt", "lte", "gt", "gte",
    ];
    key_replace.iter().for_each(|&key| {
        let from = format!("\"{}\"", key);
        let to = format!("\"${}\"", key);
        string_present = string_present.replace(&from, &to);
    });
    let query_expression: QueryExpression = serde_json::from_str(&string_present)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    Ok(query_expression)
}
