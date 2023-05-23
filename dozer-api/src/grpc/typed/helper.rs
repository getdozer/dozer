use crate::generator::protoc::generator::{
    CountResponseDesc, EventDesc, QueryResponseDesc, RecordDesc, RecordWithIdDesc,
    TokenResponseDesc,
};
use crate::grpc::types_helper::map_record;
use dozer_cache::cache::CacheRecord;
use dozer_types::grpc_types::types as GrpcTypes;
use prost_reflect::{DynamicMessage, ReflectMessage, SetFieldError, Value};

use super::TypedResponse;

pub fn on_event_to_typed_response(
    op: GrpcTypes::Operation,
    event_desc: EventDesc,
) -> Result<TypedResponse, SetFieldError> {
    let mut event = DynamicMessage::new(event_desc.message);
    event.try_set_field(
        &event_desc.typ_field,
        prost_reflect::Value::EnumNumber(op.typ),
    )?;
    if let Some(old) = op.old {
        event.try_set_field(
            &event_desc.old_field,
            prost_reflect::Value::Message(internal_record_to_pb(old, &event_desc.record_desc)?),
        )?;
    }

    event.try_set_field(
        &event_desc.new_field,
        prost_reflect::Value::Message(internal_record_to_pb(
            op.new.unwrap(),
            &event_desc.record_desc,
        )?),
    )?;

    if let Some(new_id) = op.new_id {
        event.try_set_field(&event_desc.new_id_field, prost_reflect::Value::U64(new_id))?;
    }

    Ok(TypedResponse::new(event))
}

fn internal_record_to_pb(
    record: GrpcTypes::Record,
    record_desc: &RecordDesc,
) -> Result<DynamicMessage, SetFieldError> {
    let mut msg = DynamicMessage::new(record_desc.message.clone());

    // `record_desc` has more fields than `record.values` because it also contains the version field.
    // Here `zip` handles the case.
    for (field, value) in record_desc.message.fields().zip(record.values.into_iter()) {
        if let Some(v) = interval_value_to_pb(value, record_desc) {
            msg.try_set_field(&field, v)?;
        }
    }

    msg.try_set_field(
        &record_desc.version_field,
        prost_reflect::Value::U32(record.version),
    )?;

    Ok(msg)
}

fn interval_value_to_pb(
    value: GrpcTypes::Value,
    descriptor: &RecordDesc,
) -> Option<prost_reflect::Value> {
    value.value.map(|value| match value {
        GrpcTypes::value::Value::UintValue(n) => Value::U64(n),
        GrpcTypes::value::Value::IntValue(n) => Value::I64(n),
        GrpcTypes::value::Value::FloatValue(n) => Value::F64(n),
        GrpcTypes::value::Value::BoolValue(n) => Value::Bool(n),
        GrpcTypes::value::Value::StringValue(n) => Value::String(n),
        GrpcTypes::value::Value::BytesValue(n) => {
            Value::Bytes(prost_reflect::bytes::Bytes::from(n))
        }
        GrpcTypes::value::Value::Uint128Value(n) | GrpcTypes::value::Value::Int128Value(n) => {
            Value::String(n)
        }
        GrpcTypes::value::Value::PointValue(p) => {
            let point_type_desc = descriptor.point_field.message.clone();
            let x_field_desc = &descriptor.point_field.x;
            let y_field_desc = &descriptor.point_field.y;
            let mut point = DynamicMessage::new(point_type_desc);
            point.set_field(x_field_desc, prost_reflect::Value::F64(p.x));
            point.set_field(y_field_desc, prost_reflect::Value::F64(p.y));
            Value::Message(point)
        }
        GrpcTypes::value::Value::DurationValue(d) => {
            let duration_type_desc = descriptor.duration_field.message.clone();
            let value_field_desc = &descriptor.duration_field.value;
            let time_unit_field_desc = &descriptor.duration_field.time_unit;
            let mut duration = DynamicMessage::new(duration_type_desc);
            duration.set_field(value_field_desc, prost_reflect::Value::String(d.value));
            duration.set_field(
                time_unit_field_desc,
                prost_reflect::Value::String(d.time_unit),
            );
            Value::Message(duration)
        }
        GrpcTypes::value::Value::DecimalValue(d) => {
            let decimal_type_desc = descriptor.decimal_field.message.clone();
            let scale_field_desc = &descriptor.decimal_field.scale;
            let lo_field_desc = &descriptor.decimal_field.lo;
            let mid_field_desc = &descriptor.decimal_field.mid;
            let hi_field_desc = &descriptor.decimal_field.hi;
            let negative_field_desc = &descriptor.decimal_field.negative;
            let mut decimal = DynamicMessage::new(decimal_type_desc);
            decimal.set_field(scale_field_desc, prost_reflect::Value::U32(d.scale));
            decimal.set_field(lo_field_desc, prost_reflect::Value::U32(d.lo));
            decimal.set_field(mid_field_desc, prost_reflect::Value::U32(d.mid));
            decimal.set_field(hi_field_desc, prost_reflect::Value::U32(d.hi));
            decimal.set_field(negative_field_desc, prost_reflect::Value::Bool(d.negative));
            Value::Message(decimal)
        }
        GrpcTypes::value::Value::TimestampValue(ts) => Value::Message(ts.transcode_to_dynamic()),
        GrpcTypes::value::Value::DateValue(d) => Value::String(d),
        GrpcTypes::value::Value::JsonValue(v) => Value::Message(v.transcode_to_dynamic()),
    })
}

fn internal_record_with_id_to_pb(
    record_with_id: CacheRecord,
    record_with_id_desc: &RecordWithIdDesc,
) -> Result<DynamicMessage, SetFieldError> {
    let mut msg = DynamicMessage::new(record_with_id_desc.message.clone());

    let record_with_id = map_record(record_with_id);

    let record = internal_record_to_pb(
        record_with_id.record.expect("Record is not optional"),
        &record_with_id_desc.record_desc,
    )?;
    msg.try_set_field(
        &record_with_id_desc.record_field,
        prost_reflect::Value::Message(record),
    )?;

    let id = prost_reflect::Value::U64(record_with_id.id as _);
    msg.try_set_field(&record_with_id_desc.id_field, id)?;

    Ok(msg)
}

pub fn count_response_to_typed_response(
    count: usize,
    response_desc: CountResponseDesc,
) -> Result<TypedResponse, SetFieldError> {
    let mut msg = DynamicMessage::new(response_desc.message);
    msg.try_set_field(
        &response_desc.count_field,
        prost_reflect::Value::U64(count as _),
    )?;

    Ok(TypedResponse::new(msg))
}

pub fn query_response_to_typed_response(
    records: Vec<CacheRecord>,
    response_desc: QueryResponseDesc,
) -> Result<TypedResponse, SetFieldError> {
    let mut msg = DynamicMessage::new(response_desc.message);

    let data: Result<Vec<prost_reflect::Value>, SetFieldError> = records
        .into_iter()
        .map(|record_with_id| {
            let record_with_id =
                internal_record_with_id_to_pb(record_with_id, &response_desc.record_with_id_desc)?;
            Ok(prost_reflect::Value::Message(record_with_id))
        })
        .collect();
    msg.try_set_field(
        &response_desc.records_field,
        prost_reflect::Value::List(data?),
    )?;
    Ok(TypedResponse::new(msg))
}

pub fn token_response(token: String, response_desc: TokenResponseDesc) -> TypedResponse {
    let mut msg = DynamicMessage::new(response_desc.message);
    msg.set_field(
        &response_desc.token_field,
        prost_reflect::Value::String(token),
    );
    TypedResponse::new(msg)
}
