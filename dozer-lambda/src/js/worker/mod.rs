use std::{num::NonZeroI32, sync::Arc};

use dozer_log::tokio::runtime::Runtime;
use dozer_types::{
    json_types::{field_to_json_value, json, JsonObject, JsonValue},
    log::error,
    types::{Field, Operation},
};

#[derive(Debug)]
pub struct Worker {
    runtime: dozer_deno::Runtime,
}

impl Worker {
    pub async fn new(
        runtime: Arc<Runtime>,
        modules: Vec<String>,
    ) -> Result<(Self, Vec<NonZeroI32>), dozer_deno::RuntimeError> {
        let (runtime, lambdas) = dozer_deno::Runtime::new(runtime, modules).await?;
        Ok((Self { runtime }, lambdas))
    }

    pub async fn call_lambda(
        &mut self,
        func: NonZeroI32,
        operation_index: u64,
        operation: Operation,
        field_names: Vec<String>,
    ) {
        let (operation_type, new_values, old_values) = match operation {
            Operation::Insert { new } => ("insert", new.values, None),
            Operation::Update { new, old } => ("update", new.values, Some(old.values)),
            Operation::Delete { old } => ("delete", old.values, None),
        };
        let arg = json!({
            "type": operation_type,
            "index": operation_index,
            "new": create_record_json_value(field_names.clone(), new_values),
            "old": old_values.map(|old_values| create_record_json_value(field_names, old_values)),
        });
        if let Err(e) = self.runtime.call_function(func, vec![arg]).await {
            error!("error calling lambda: {}", e);
        }
    }
}

fn create_record_json_value(field_names: Vec<String>, values: Vec<Field>) -> JsonValue {
    let values = values.into_iter().map(field_to_json_value);
    field_names
        .into_iter()
        .zip(values)
        .collect::<JsonObject>()
        .into()
}
