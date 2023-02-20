use std::borrow::Cow;
use crate::generator::protoc::generator::{
    CountResponseDesc, EventDesc, QueryResponseDesc, RecordDesc, RecordWithIdDesc,
    TokenResponseDesc,
};
use crate::grpc::types::{self as GrpcTypes, PointType};
use crate::grpc::types_helper::map_record;
use dozer_cache::cache::RecordWithId;
use prost_reflect::{DynamicMessage, Value};
use prost_reflect::Value::Message;
use crate::grpc::types::value::Value::PointValue;

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
            prost_reflect::Value::Message(internal_record_to_pb(old, &event_desc.record_desc)),
        );
    }

    if let Some(new) = op.new {
        event.set_field(
            &event_desc.new_field,
            prost_reflect::Value::Message(internal_record_to_pb(new, &event_desc.record_desc)),
        );
    }

    TypedResponse::new(event)
}

fn internal_record_to_pb(record: GrpcTypes::Record, record_desc: &RecordDesc) -> DynamicMessage {
    let mut msg = DynamicMessage::new(record_desc.message.clone());

    // `record_desc` has more fields than `record.values` because it also contains the version field.
    // Here `zip` handles the case.
    for (field, value) in record_desc.message.fields().zip(record.values.into_iter()) {
        let reflect_value = match &value.value {
            None => None,
            Some(PointValue(point)) => point_to_pb(&msg, field.name(), point),
            Some(_) => interval_value_to_pb(value)
        };

        if let Some(value) = reflect_value {
            msg.set_field(&field, value);
        }
    }

    msg.set_field(
        &record_desc.version_field,
        prost_reflect::Value::U32(record.version),
    );

    msg
}

fn point_to_pb(msg: &DynamicMessage, field_name: &str, point: &PointType) -> Option<Value> {
    let val = msg.get_field_by_name(field_name).expect("Field should present in message");
    match val {
        Cow::Borrowed(v) => {
            match v {
                Message(m) => {
                    let mut m = m.clone();
                    m.set_field_by_name("x", Value::F64(point.x));
                    m.set_field_by_name("y", Value::F64(point.y));
                    Some(Message(m))
                },
                v => Some(v.clone())
            }
        }
        Cow::Owned(v) => {
            match v {
                Message(mut m) => {
                    m.set_field_by_name("x", Value::F64(point.x));
                    m.set_field_by_name("y", Value::F64(point.y));
                    Some(Message(m))
                },
                v => Some(v)
            }
        }
    }
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

fn internal_record_with_id_to_pb(
    record_with_id: RecordWithId,
    record_with_id_desc: &RecordWithIdDesc,
) -> DynamicMessage {
    let mut msg = DynamicMessage::new(record_with_id_desc.message.clone());

    let record_with_id = map_record(record_with_id);

    let record = internal_record_to_pb(
        record_with_id.record.expect("Record is not optional"),
        &record_with_id_desc.record_desc,
    );
    msg.set_field(
        &record_with_id_desc.record_field,
        prost_reflect::Value::Message(record),
    );

    let id = prost_reflect::Value::U64(record_with_id.id as _);
    msg.set_field(&record_with_id_desc.id_field, id);

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
        .map(|record_with_id| {
            let record_with_id =
                internal_record_with_id_to_pb(record_with_id, &response_desc.record_with_id_desc);
            prost_reflect::Value::Message(record_with_id)
        })
        .collect::<Vec<_>>();
    msg.set_field(
        &response_desc.records_field,
        prost_reflect::Value::List(data),
    );
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
