use dozer_api::rest::field_to_json_value;
use dozer_core::executor::ExecutorOperation;
use dozer_types::{
    serde_json::Value,
    types::{Field, Operation, Record, Schema},
};
use neon::{
    prelude::{Context, Object},
    result::JsResult,
    types::{JsObject, JsValue},
};

pub fn map_executor_operation<'a, C: Context<'a>>(
    op: ExecutorOperation,
    schema: &Schema,
    cx: &mut C,
) -> JsResult<'a, JsObject> {
    let result = cx.empty_object();

    match op {
        ExecutorOperation::Op { op } => {
            let op = map_operation(op, schema, cx)?;
            let typ = cx.string("op");
            result.set(cx, "type", typ)?;
            result.set(cx, "op", op)?;
        }
        ExecutorOperation::Commit { .. } => {
            let typ = cx.string("commit");
            result.set(cx, "type", typ)?;
        }
        ExecutorOperation::SnapshottingDone {} => {
            let typ = cx.string("snapshotting_done");
            result.set(cx, "type", typ)?;
        }
        ExecutorOperation::Terminate => {
            let typ = cx.string("terminate");
            result.set(cx, "type", typ)?;
        }
    }

    Ok(result)
}

fn map_operation<'a, C: Context<'a>>(
    op: Operation,
    schema: &Schema,
    cx: &mut C,
) -> JsResult<'a, JsObject> {
    let result = cx.empty_object();

    match op {
        Operation::Insert { new } => {
            let typ = cx.string("insert");
            result.set(cx, "type", typ)?;
            let new = map_record(new, schema, cx)?;
            result.set(cx, "new", new)?;
        }
        Operation::Delete { old } => {
            let typ = cx.string("delete");
            result.set(cx, "type", typ)?;
            let old = map_record(old, schema, cx)?;
            result.set(cx, "old", old)?;
        }
        Operation::Update { old, new } => {
            let typ = cx.string("update");
            result.set(cx, "type", typ)?;
            let old = map_record(old, schema, cx)?;
            result.set(cx, "old", old)?;
            let new = map_record(new, schema, cx)?;
            result.set(cx, "new", new)?;
        }
    }

    Ok(result)
}

fn map_record<'a, C: Context<'a>>(
    record: Record,
    schema: &Schema,
    cx: &mut C,
) -> JsResult<'a, JsObject> {
    let result = cx.empty_object();
    for (field, value) in schema.fields.iter().zip(record.values) {
        let value = map_value(value, cx)?;
        result.set(cx, field.name.as_str(), value)?;
    }
    Ok(result)
}

fn map_value<'a, C: Context<'a>>(value: Field, cx: &mut C) -> JsResult<'a, JsValue> {
    let value = field_to_json_value(value);
    map_json_value(value, cx)
}

fn map_json_value<'a, C: Context<'a>>(value: Value, cx: &mut C) -> JsResult<'a, JsValue> {
    match value {
        Value::Null => Ok(cx.null().upcast()),
        Value::Bool(b) => Ok(cx.boolean(b).upcast()),
        Value::Number(n) => Ok(cx.number(n.as_f64().unwrap_or(f64::NAN)).upcast()),
        Value::String(s) => Ok(cx.string(s).upcast()),
        Value::Array(a) => {
            let result = cx.empty_array();
            for (i, v) in a.into_iter().enumerate() {
                let v = map_json_value(v, cx)?;
                result.set(cx, i as u32, v)?;
            }
            Ok(result.upcast())
        }
        Value::Object(o) => {
            let result = cx.empty_object();
            for (k, v) in o.into_iter() {
                let v = map_json_value(v, cx)?;
                result.set(cx, k.as_str(), v)?;
            }
            Ok(result.upcast())
        }
    }
}
