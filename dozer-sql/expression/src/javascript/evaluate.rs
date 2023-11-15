use std::{num::NonZeroI32, sync::Arc};

use dozer_deno::deno_runtime::deno_core::error::AnyError;
use dozer_types::{
    errors::types::DeserializationError,
    json_types::{json_value_to_serde_json, serde_json_to_json_value},
    thiserror,
    types::{Field, FieldType, Record, Schema, SourceDefinition},
};
use tokio::{runtime::Runtime, sync::Mutex};

use crate::execution::{Expression, ExpressionType};

#[derive(Debug, Clone)]
pub struct Udf {
    function_name: String,
    arg: Box<Expression>,
    tokio_runtime: Arc<Runtime>,
    /// Always `Some`. `Arc<Mutex>` to enable `Clone`. Not sure why `Expression` should be `Clone`.
    deno_runtime: Arc<Mutex<Option<dozer_deno::Runtime>>>,
    function: NonZeroI32,
}

impl PartialEq for Udf {
    fn eq(&self, other: &Self) -> bool {
        // This is obviously wrong. We have to lift the `PartialEq` constraint.
        self.function_name == other.function_name && self.arg == other.arg
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to create deno runtime: {0}")]
    CreateRuntime(#[from] dozer_deno::RuntimeError),
    #[error("failed to evaluate udf: {0}")]
    Evaluate(#[source] AnyError),
    #[error("failed to convert json: {0}")]
    JsonConversion(#[source] DeserializationError),
}

impl Udf {
    pub async fn new(
        tokio_runtime: Arc<Runtime>,
        function_name: String,
        module: String,
        arg: Expression,
    ) -> Result<Self, Error> {
        let (deno_runtime, functions) =
            dozer_deno::Runtime::new(tokio_runtime.clone(), vec![module]).await?;
        let function = functions[0];
        Ok(Self {
            function_name,
            arg: Box::new(arg),
            tokio_runtime,
            deno_runtime: Arc::new(Mutex::new(Some(deno_runtime))),
            function,
        })
    }

    pub fn get_type(&self) -> ExpressionType {
        ExpressionType {
            return_type: FieldType::Json,
            nullable: false,
            source: SourceDefinition::Dynamic,
            is_primary_key: false,
        }
    }

    pub fn evaluate(
        &mut self,
        record: &Record,
        schema: &Schema,
    ) -> Result<Field, crate::error::Error> {
        self.tokio_runtime.block_on(evaluate_impl(
            self.function_name.clone(),
            &mut self.arg,
            &self.deno_runtime,
            self.function,
            record,
            schema,
        ))
    }

    pub fn to_string(&self, schema: &Schema) -> String {
        format!("{}({})", self.function_name, self.arg.to_string(schema))
    }
}

async fn evaluate_impl(
    function_name: String,
    arg: &mut Expression,
    runtime: &Arc<Mutex<Option<dozer_deno::Runtime>>>,
    function: NonZeroI32,
    record: &Record,
    schema: &Schema,
) -> Result<Field, crate::error::Error> {
    let arg = arg.evaluate(record, schema)?;
    let Field::Json(arg) = arg else {
        return Err(crate::error::Error::InvalidFunctionArgument {
            function_name,
            argument_index: 0,
            argument: arg,
        });
    };

    let mut runtime = runtime.lock().await;
    let result = runtime
        .take()
        .unwrap()
        .call_function(function, vec![json_value_to_serde_json(&arg)])
        .await;
    *runtime = Some(result.0);
    drop(runtime);

    let result = result.1.map_err(Error::Evaluate)?;
    let result = serde_json_to_json_value(result).map_err(Error::JsonConversion)?;
    Ok(Field::Json(result))
}
