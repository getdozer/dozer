pub mod error;
pub mod udf;
pub mod utils;
use std::fmt;

use wasmtime::*;

#[derive(Clone)]
pub struct WasmSession {
    /// Used just for printing errors
    pub module_path: String,
    pub function_name: String,
    pub instance_pre: InstancePre<()>,
    pub engine: Engine,
    pub value_types: Vec<ValType>,
    pub return_type: ValType,
}

/// Debug implementation for WasmSession.
/// `instance_pre` is omitted from this implementation.
impl fmt::Debug for WasmSession {
   fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmSession")
            .field("module_path", &self.function_name)
            .field("function_name", &self.function_name)
            // Omit the debug values for `InstancePre` and `Engine`
            .field("value_types", &self.value_types)
            .field("return_type", &self.return_type)
            .finish()
   }
}

impl PartialEq for WasmSession {
    fn eq(&self, other: &Self) -> bool {
        self.value_types == other.value_types && self.return_type == other.return_type
    }
}
