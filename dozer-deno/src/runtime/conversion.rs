use std::ops::Deref;

use deno_core::{
    anyhow::{bail, Context as _},
    error::AnyError,
};
use deno_napi::v8::{self, HandleScope, Local};
use dozer_types::json_types::{DestructuredJson, JsonObject, JsonValue};

pub fn to_v8<'s>(
    scope: &mut HandleScope<'s>,
    value: JsonValue,
) -> Result<Local<'s, v8::Value>, AnyError> {
    match value.destructure() {
        DestructuredJson::Null => Ok(v8::null(scope).into()),
        DestructuredJson::Bool(value) => Ok(v8::Boolean::new(scope, value).into()),
        DestructuredJson::Number(value) => {
            let value = value.to_f64().context("number is not a f64")?;
            Ok(v8::Number::new(scope, value).into())
        }
        DestructuredJson::String(value) => Ok(v8::String::new(scope, &value)
            .context(format!("failed to create string {}", value.deref()))?
            .into()),
        DestructuredJson::Array(values) => {
            let array = v8::Array::new(scope, values.len() as i32);
            for (index, value) in values.into_iter().enumerate() {
                let value = to_v8(scope, value)?;
                array.set_index(scope, index as u32, value);
            }
            Ok(array.into())
        }
        DestructuredJson::Object(map) => {
            let object = v8::Object::new(scope);
            for (key, value) in map.into_iter() {
                let key = v8::String::new(scope, &key)
                    .context(format!("failed to create key {}", key.deref()))?;
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
) -> Result<JsonValue, AnyError> {
    if value.is_null_or_undefined() {
        Ok(JsonValue::NULL)
    } else if value.is_boolean() {
        Ok(value.boolean_value(scope).into())
    } else if value.is_number() {
        Ok(value
            .number_value(scope)
            .context("number is not a f64")?
            .into())
    } else if let Ok(value) = TryInto::<Local<v8::String>>::try_into(value) {
        Ok(value.to_rust_string_lossy(scope).into())
    } else if let Ok(value) = TryInto::<Local<v8::Array>>::try_into(value) {
        let mut values = Vec::new();
        for index in 0..value.length() {
            let value = value.get_index(scope, index).unwrap();
            let value = from_v8(scope, value)?;
            values.push(value);
        }
        Ok(values.into())
    } else if let Ok(value) = TryInto::<Local<v8::Object>>::try_into(value) {
        let mut map = JsonObject::new();
        let Some(keys) = value.get_own_property_names(scope, Default::default()) else {
            return Ok(map.into());
        };
        for index in 0..keys.length() {
            let key = keys.get_index(scope, index).unwrap();
            let value = value.get(scope, key).unwrap();
            let key = key.to_rust_string_lossy(scope);
            let value = from_v8(scope, value)?;
            map.insert(key, value);
        }
        Ok(map.into())
    } else {
        bail!("cannot convert v8 value to JSON because its type is not supported")
    }
}
