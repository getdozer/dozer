use dozer_log::ExecutorOperation;

use dozer_types::chrono::SecondsFormat;

use dozer_types::serde::{self, Deserialize, Serialize};
use dozer_types::types::{Field, Record, Schema};
use dozer_types::{
    crossbeam::channel::{Receiver, Sender},
    serde_json,
    types::Operation,
};

use neon::prelude::*;

#[derive(Clone)]
pub enum ControlMessage {
    Stop,
    Read,
}

#[derive(Clone)]
pub struct LogReaderWrapper {
    pub sender: Sender<ControlMessage>,
    pub msg_receiver: Receiver<Option<ExecutorOperation>>,
    pub schema: Schema,
}

impl Finalize for LogReaderWrapper {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
pub enum ReadMsgType {
    Insert,
    Delete,
    Update,
    Control,
}

pub fn map_read_operation<'a>(
    op: ExecutorOperation,
    schema: &Schema,
    cx: &mut TaskContext<'a>,
) -> JsResult<'a, JsObject> {
    match op {
        ExecutorOperation::Op { op } => match op {
            Operation::Delete { old } => {
                let obj = cx.empty_object();
                let typ = cx.string("delete");
                let old = record_to_obj(old, schema, cx)?;
                obj.set(cx, "type", typ)?;
                obj.set(cx, "old", old)?;
                Ok(obj)
            }
            Operation::Insert { new } => {
                let obj = cx.empty_object();
                let typ = cx.string("insert");
                let new = record_to_obj(new, schema, cx)?;

                obj.set(cx, "type", typ)?;
                obj.set(cx, "new", new)?;
                Ok(obj)
            }
            Operation::Update { old, new } => {
                let obj = cx.empty_object();
                let typ = cx.string("update");
                let new = record_to_obj(new, schema, cx)?;
                let old = record_to_obj(old, schema, cx)?;

                obj.set(cx, "type", typ)?;
                obj.set(cx, "new", new)?;
                obj.set(cx, "old", old)?;
                Ok(obj)
            }
        },
        msg => {
            let obj = cx.empty_object();

            let typ = cx.string("control");
            let msg = match msg {
                ExecutorOperation::Commit { epoch: _ } => "Commit",
                ExecutorOperation::Terminate => "Terminate",
                ExecutorOperation::SnapshottingDone {} => "SnapshottingDone",
                _ => "Unexpected",
            };
            let msg = cx.string(msg);
            obj.set(cx, "type", typ)?;
            obj.set(cx, "msg", msg)?;
            Ok(obj)
        }
    }
}

pub const DATE_FORMAT: &str = "%Y-%m-%d";

/// Used in REST APIs for converting to JSON
fn record_to_obj<'a>(
    record: Record,
    schema: &Schema,
    cx: &mut TaskContext<'a>,
) -> JsResult<'a, JsObject> {
    let obj = cx.empty_object();

    for (field_def, field) in schema.fields.iter().zip(record.values) {
        match field {
            Field::UInt(n) => {
                let val = cx.number(n as f64);
                obj.set(cx, &*field_def.name, val)?;
            }
            Field::U128(n) => {
                let val = cx.number(n as f64);
                obj.set(cx, &*field_def.name, val)?;
            }
            Field::Int(n) => {
                let val = cx.number(n as f64);
                obj.set(cx, &*field_def.name, val)?;
            }
            Field::I128(n) => {
                let val = cx.number(n as f64);
                obj.set(cx, &*field_def.name, val)?;
            }
            Field::Float(n) => {
                let val = cx.number(n.0 as f64);
                obj.set(cx, &*field_def.name, val)?;
            }
            Field::Boolean(b) => {
                let val = cx.boolean(b);
                obj.set(cx, &*field_def.name, val)?;
            }
            Field::String(s) | Field::Text(s) => {
                let val = cx.string(s);
                obj.set(cx, &*field_def.name, val)?;
            }

            Field::Binary(b) | Field::Bson(b) => {
                let val = cx.string(format!("{:?}", b));
                obj.set(cx, &*field_def.name, val)?;
            }

            Field::Decimal(n) => {
                let val = cx.string(n.to_string());
                obj.set(cx, &*field_def.name, val)?;
            }
            Field::Timestamp(ts) => {
                let ts = cx.string(ts.to_rfc3339_opts(SecondsFormat::Millis, true));
                obj.set(cx, &*field_def.name, ts)?;
            }
            Field::Date(n) => {
                let s = cx.string(n.format(DATE_FORMAT).to_string());
                obj.set(cx, &*field_def.name, s)?;
            }

            Field::Point(point) => {
                let point = cx.string(serde_json::to_string(&point).unwrap());
                obj.set(cx, &*field_def.name, point)?;
            }
            Field::Duration(d) => {
                let d = cx.string(serde_json::to_string(&d).unwrap());
                obj.set(cx, &*field_def.name, d)?;
            }
            Field::Null => {
                let val = cx.null();
                obj.set(cx, &*field_def.name, val)?;
            }
        }
    }

    Ok(obj)
}
