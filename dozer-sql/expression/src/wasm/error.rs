use dozer_types::types::Field;
use dozer_types::thiserror::{self, Error};
use crate::execution::Expression;

#[derive(Debug, Error)]
pub enum Error {
    #[error(
        "Wasm UDF must have a return type. The syntax is: function_name<return_type>(arguments)"
    )]
    MissingReturnType,
    #[error("wasmtime error: {0}")]
    WasmTime(#[from] wasmtime::Error),
    #[error("Input type {0} is not yet supported in Wasm")]
    WasmUnsupportedInputType(String),
    #[error("Return type {0} is not yet supported in Wasm")]
    WasmUnsupportedReturnType(String),

    #[error("Expected wasm input datatype {0} doesn't match with actual input datatype {1}")]
    WasmInputDataTypeMismatchErr(String, String),
    #[error("Dozer doesn't support following output datatype {0:?}")]
    WasmNotSupportedDataTypeErr(String),
    #[error("Dozer can't find following column in the input schema {0:?}")]
    ColumnNotFound(Expression),
    #[error("Input argument overflow for {1:?}: {0}")]
    InputArgumentOverflow(Field, String),
    #[error("execution of WASM UDF function {0} resulted in a WASM trap: {1}")]
    WasmTrap(String, String),
    #[error("The WASM return type must be a single value, for now, not {0}")]
    WasmUnsupportedReturnTypeSize(usize),
    #[error("The WASM input type must match the number of arguments received: {0} != {1}")]
    WasmInputTypeSizeMismatch(usize, usize),
    #[error("The WASM function {0} is missing from the module {1}")]
    WasmFunctionMissing(String, String),

}
