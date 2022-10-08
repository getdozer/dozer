use crate::types::{self, Record, Schema};
use anyhow::{bail, Context};
use rust_decimal::Decimal;
use serde_json::Value;
use std::{collections::HashMap, str};
use types::Field;

pub fn json_value_to_field(val: Value) -> anyhow::Result<Field> {
    let val: Field = match val {
        Value::Null => Field::Null,
        Value::Bool(b) => Field::Boolean(b),
        Value::Number(no) => {
            if no.is_u64() {
                let val = no.as_i64().context("conversion to u64 failed")?;
                Field::Int(val)
            } else if no.is_i64() {
                let val = no.as_i64().context("conversion to i64 failed")?;
                Field::Int(val)
            } else if no.is_f64() {
                let val = no.as_f64().context("conversion to f64 failed")?;
                Field::Float(val)
            } else {
                //TODO: test this
                let decimal = Decimal::from_str_exact(&no.to_string())?;
                Field::Decimal(decimal)
            }
        }
        Value::String(str) => Field::String(str),
        Value::Array(_arr) => {
            bail!("arr is not handled")
        }
        Value::Object(_obj) => bail!("obj is not handled"),
    };
    Ok(val)
}

pub fn field_to_json_value(field: &Field) -> anyhow::Result<Value> {
    let val = match field {
        Field::Int(n) => Value::from(*n),
        Field::Float(n) => Value::from(*n),
        // TODO
        Field::Boolean(b) => Value::from(*b),
        Field::String(s) => Value::String(s.clone()),
        Field::Binary(b) => Value::String(
            str::from_utf8(&b)
                .context("cannot convert to string")?
                .to_string(),
        ),
        Field::Null => Value::Null,
        Field::Decimal(_n) => todo!(),
        Field::Timestamp(_ts) => todo!(),
        Field::Bson(_) => todo!(),
        Field::RecordArray(_) => todo!(),

        Field::Invalid(_) => todo!(),
    };
    Ok(val)
}

pub fn record_to_json(rec: &Record, schema: &Schema) -> anyhow::Result<HashMap<String, Value>> {
    let mut map: HashMap<String, Value> = HashMap::new();
    let mut idx: usize = 0;
    for field_def in schema.fields.iter() {
        let field = rec.values[idx].clone();
        let val: Value = field_to_json_value(&field)?;
        map.insert(field_def.name.clone(), val);
        idx += 1;
    }

    Ok(map)
}
