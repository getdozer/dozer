use std::{num::NonZeroI32, sync::Arc};

use dozer_log::tokio::runtime::Runtime;
use dozer_types::{
    json_types::field_to_json_value,
    log::error,
    serde_json::{json, Value},
    types::{Field, Operation},
};

#[derive(Debug)]
pub struct Worker {
    /// Always `Some`.
    runtime: Option<dozer_deno::Runtime>,
}

impl Worker {
    pub async fn new(
        runtime: Arc<Runtime>,
        modules: Vec<String>,
    ) -> Result<(Self, Vec<NonZeroI32>), dozer_deno::RuntimeError> {
        let (runtime, lambdas) = dozer_deno::Runtime::new(runtime, modules).await?;
        Ok((
            Self {
                runtime: Some(runtime),
            },
            lambdas,
        ))
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
        let result = self
            .runtime
            .take()
            .unwrap()
            .call_function(func, vec![arg])
            .await;
        self.runtime = Some(result.0);
        if let Err(e) = result.1 {
            error!("error calling lambda: {}", e);
        }
    }
}

fn create_record_json_value(field_names: Vec<String>, values: Vec<Field>) -> Value {
    let mut record = Value::Object(Default::default());
    for (field_name, value) in field_names.into_iter().zip(values.into_iter()) {
        record[field_name] = field_to_json_value(value);
    }
    record
}
