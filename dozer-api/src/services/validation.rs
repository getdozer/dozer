use serde::de::DeserializeOwned;
use serde_json::{Error, Value};
use std::{any::type_name, fs::File};
use valico::json_schema;

pub fn validation<T>(value: Value) -> T
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

    println!("=== validation_state: {:?}", validation_state);
    if validation_state.errors.len() < 1 {
        serde_json::from_value::<T>(value).unwrap()
    } else {
        panic!("{:?}", serde_json::to_string(&validation_state))
    }
}
