use std::{num::NonZeroI32, sync::Arc};

use dozer_core::{
    checkpoint::serialize::{
        deserialize_vec_u8, serialize_vec_u8, Cursor, DeserializationError, SerializationError,
    },
    dozer_log::storage::Object,
};
use dozer_deno::deno_runtime::deno_core::{self, error::AnyError, extension, op2};
use dozer_types::{
    json_types::JsonValue,
    parking_lot, serde_json, thiserror,
    types::{Field, FieldType, Record, Schema, SourceDefinition},
};
use tokio::{runtime::Runtime, sync::Mutex};

use crate::execution::{Expression, ExpressionType};

#[derive(Debug, Clone)]
pub struct Udf {
    function_name: String,
    arg: Box<Expression>,
    tokio_runtime: Arc<Runtime>,
    /// `Arc<Mutex>` to enable `Clone`. Not sure why `Expression` should be `Clone`.
    deno_runtime: Arc<Mutex<dozer_deno::Runtime>>,
    function: NonZeroI32,
    state: Arc<parking_lot::Mutex<JsonValue>>,
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
    #[error("serialization: {0}")]
    Serialization(#[from] SerializationError),
    #[error("deserialization: {0}")]
    Deserialization(#[from] DeserializationError),
    #[error("serde json: {0}")]
    SerdeJson(#[from] serde_json::Error),
}

#[op2]
fn set_state(#[state] state: &Arc<parking_lot::Mutex<JsonValue>>, #[serde] new_state: JsonValue) {
    *state.lock() = new_state;
}

#[op2]
#[serde]
fn get_state(#[state] state: &Arc<parking_lot::Mutex<JsonValue>>) -> JsonValue {
    state.lock().clone()
}

extension!(
    dozer_udf,
    ops = [set_state, get_state],
    options = { state: Arc<parking_lot::Mutex<JsonValue>> },
    state = |state, options| {
        state.put(options.state);
    },
);

impl Udf {
    pub async fn new(
        tokio_runtime: Arc<Runtime>,
        function_name: String,
        module: String,
        arg: Expression,
    ) -> Result<Self, Error> {
        let state = Arc::new(parking_lot::Mutex::new(JsonValue::NULL));
        let state_clone = state.clone();
        let (deno_runtime, functions) =
            dozer_deno::Runtime::new(vec![module], vec![move || dozer_udf::init_ops(state)])
                .await?;
        let function = functions[0];
        Ok(Self {
            function_name,
            arg: Box::new(arg),
            tokio_runtime,
            deno_runtime: Arc::new(Mutex::new(deno_runtime)),
            function,
            state: state_clone,
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

    pub fn serialize(&self, object: &mut Object) -> Result<(), Error> {
        let bytes = serde_json::to_vec(&*self.state.lock()).expect("must succeed");
        serialize_vec_u8(&bytes, object)?;
        Ok(())
    }

    pub fn deserialize(&mut self, cursor: &mut Cursor) -> Result<(), Error> {
        let bytes = deserialize_vec_u8(cursor)?;
        *self.state.lock() = serde_json::from_slice(bytes)?;
        Ok(())
    }
}

async fn evaluate_impl(
    function_name: String,
    arg: &mut Expression,
    runtime: &Arc<Mutex<dozer_deno::Runtime>>,
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
        .call_function(function, vec![arg])
        .await
        .map_err(Error::Evaluate)?;
    Ok(Field::Json(result))
}
