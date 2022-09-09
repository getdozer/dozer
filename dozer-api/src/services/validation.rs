use crate::errors::validation_error::ValidationError;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{any::type_name, fs::File};
use valico::json_schema::{self, ValidationState};
pub fn validation<T>(value: Value) -> Result<T, ValidationError<ValidationState>>
where
    T: DeserializeOwned,
{
    let full_name = type_name::<T>().to_string();
    let name = full_name.split("::").last().unwrap().to_string();
    let json_schema: Value = serde_json::from_reader(
        File::open(format!("src/models/json-schema/{}.json", name)).unwrap(),
    )
    .unwrap();
    let mut scope = json_schema::Scope::new();
    let schema = scope
        .compile_and_return(json_schema.clone(), false)
        .unwrap();
    let validation_state = schema.validate(&value);
    if validation_state.errors.len() < 1 {
        Ok(serde_json::from_value::<T>(value).unwrap())
    } else {
        Err(ValidationError {
            details: validation_state,
            message: format!("Validate {}", name),
        })
    }
}
