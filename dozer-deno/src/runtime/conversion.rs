use deno_runtime::{
    deno_core::{
        anyhow::{bail, Context as _},
        error::AnyError,
    },
    deno_napi::v8::{self, HandleScope, Local},
};
use dozer_types::serde_json::{self, Number};

pub fn to_v8<'s>(
    scope: &mut HandleScope<'s>,
    value: serde_json::Value,
) -> Result<Local<'s, v8::Value>, AnyError> {
    match value {
        serde_json::Value::Null => Ok(v8::null(scope).into()),
        serde_json::Value::Bool(value) => Ok(v8::Boolean::new(scope, value).into()),
        serde_json::Value::Number(value) => {
            let value = value.as_f64().context("number is not a f64")?;
            Ok(v8::Number::new(scope, value).into())
        }
        serde_json::Value::String(value) => Ok(v8::String::new(scope, &value)
            .context(format!("failed to create string {}", value))?
            .into()),
        serde_json::Value::Array(values) => {
            let array = v8::Array::new(scope, values.len() as i32);
            for (index, value) in values.into_iter().enumerate() {
                let value = to_v8(scope, value)?;
                array.set_index(scope, index as u32, value);
            }
            Ok(array.into())
        }
        serde_json::Value::Object(map) => {
            let object = v8::Object::new(scope);
            for (key, value) in map.into_iter() {
                let key = v8::String::new(scope, &key)
                    .context(format!("failed to create key {}", key))?;
                let value = to_v8(scope, value)?;
                object.set(scope, key.into(), value);
            }
            Ok(object.into())
        }
    }
}

pub fn from_v8<'s>(
    scope: &mut HandleScope<'s>,
    value: Local<'s, v8::Value>,
) -> Result<serde_json::Value, AnyError> {
    if value.is_null_or_undefined() {
        Ok(serde_json::Value::Null)
    } else if value.is_boolean() {
        Ok(serde_json::Value::Bool(value.boolean_value(scope)))
    } else if value.is_number() {
        Ok(serde_json::Value::Number(
            Number::from_f64(value.number_value(scope).context("number is not a f64")?)
                .context("f64 number cannot be represented in JSON")?,
        ))
    } else if let Ok(value) = TryInto::<Local<v8::String>>::try_into(value) {
        Ok(serde_json::Value::String(value.to_rust_string_lossy(scope)))
    } else if let Ok(value) = TryInto::<Local<v8::Array>>::try_into(value) {
        let mut values = Vec::new();
        for index in 0..value.length() {
            let value = value.get_index(scope, index).unwrap();
            let value = from_v8(scope, value)?;
            values.push(value);
        }
        Ok(serde_json::Value::Array(values))
    } else if let Ok(value) = TryInto::<Local<v8::Object>>::try_into(value) {
        let mut map = serde_json::Map::new();
        let Some(keys) = value.get_own_property_names(scope, Default::default()) else {
            return Ok(serde_json::Value::Object(map));
        };
        for index in 0..keys.length() {
            let key = keys.get_index(scope, index).unwrap();
            let value = value.get(scope, key).unwrap();
            let key = key.to_rust_string_lossy(scope);
            let value = from_v8(scope, value)?;
            map.insert(key, value);
        }
        Ok(serde_json::Value::Object(map))
    } else {
        bail!("cannot convert v8 value to JSON because its type is not supported")
    }
}
