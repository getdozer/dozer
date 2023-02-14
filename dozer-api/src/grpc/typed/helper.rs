use crate::generator::protoc::generator::{
    CountResponseDesc, EventDesc, QueryResponseDesc, TokenResponseDesc,
};
use crate::grpc::types::{self as GrpcTypes};
use crate::grpc::types_helper::field_to_prost_value;
use dozer_cache::cache::RecordWithId;
use dozer_types::types::Record;
use prost_reflect::MessageDescriptor;
use prost_reflect::{DynamicMessage, Value};

use super::TypedResponse;

pub fn on_event_to_typed_response(
    op: GrpcTypes::Operation,
    event_desc: EventDesc,
) -> TypedResponse {
    let mut event = DynamicMessage::new(event_desc.message);
    event.set_field(
        &event_desc.typ_field,
        prost_reflect::Value::EnumNumber(op.typ),
    );
    if let Some(old) = op.old {
        event.set_field(
            &event_desc.old_field,
            prost_reflect::Value::Message(internal_record_to_pb(
                old,
                &event_desc.record_desc.message,
            )),
        );
    }

    if let Some(new) = op.new {
        event.set_field(
            &event_desc.new_field,
            prost_reflect::Value::Message(internal_record_to_pb(
                new,
                &event_desc.record_desc.message,
            )),
        );
    }

    TypedResponse::new(event)
}

fn internal_record_to_pb(rec: GrpcTypes::Record, desc: &MessageDescriptor) -> DynamicMessage {
    let mut msg = DynamicMessage::new(desc.clone());

    for (field, value) in desc.fields().zip(rec.values.into_iter()) {
        if let Some(value) = interval_value_to_pb(value) {
            msg.set_field(&field, value);
        }
    }
    msg
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
    let mut msg = DynamicMessage::new(desc.clone());
    for (field, value) in desc.fields().zip(record.values.into_iter()) {
        if let Some(value) = interval_value_to_pb(field_to_prost_value(value)) {
            msg.set_field(&field, value);
        }
    }
    msg
}

pub fn count_response_to_typed_response(
    count: usize,
    response_desc: CountResponseDesc,
) -> TypedResponse {
    let mut msg = DynamicMessage::new(response_desc.message);
    msg.set_field(
        &response_desc.count_field,
        prost_reflect::Value::U64(count as _),
    );

    TypedResponse::new(msg)
}

pub fn query_response_to_typed_response(
    records: Vec<RecordWithId>,
    response_desc: QueryResponseDesc,
) -> TypedResponse {
    let mut msg = DynamicMessage::new(response_desc.message);

    let data = records
        .into_iter()
        .map(|rec| {
            prost_reflect::Value::Message(record_to_pb(
                rec.record,
                &response_desc.record_desc.message,
            ))
        })
        .collect::<Vec<_>>();
    msg.set_field(&response_desc.data_field, prost_reflect::Value::List(data));
    TypedResponse::new(msg)
}

pub fn token_response(token: String, response_desc: TokenResponseDesc) -> TypedResponse {
    let mut msg = DynamicMessage::new(response_desc.message);
    msg.set_field(
        &response_desc.token_field,
        prost_reflect::Value::String(token),
    );
    TypedResponse::new(msg)
}
