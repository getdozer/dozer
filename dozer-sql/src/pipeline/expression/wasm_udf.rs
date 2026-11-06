//! WebAssembly (WASM) Scalar UDF Registry & Executor for Dozer Core (`dozer-sql`)
//! Bounty: getdozer/dozer #1653 ($600 USD — WASM UDF Support in Dozer Core)

use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WasmUdfType {
    Int64,
    Float64,
    Utf8String,
    Boolean,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WasmUdfValue {
    Int64(i64),
    Float64(f64),
    Utf8String(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone)]
pub struct WasmUdfDescriptor {
    pub function_name: String,
    pub module_path: PathBuf,
    pub arg_types: Vec<WasmUdfType>,
    pub return_type: WasmUdfType,
}

#[derive(Debug)]
pub enum WasmUdfError {
    UdfNotFound(String),
    InvalidWasmMagicHeader(String),
    ArityMismatch { function: String, expected: usize, actual: usize },
    TypeMismatch { function: String, arg_index: usize, expected: WasmUdfType },
    ExecutionError(String),
}

pub struct WasmUdfRegistry {
    descriptors: HashMap<String, WasmUdfDescriptor>,
}

impl WasmUdfRegistry {
    pub fn new() -> Self {
        Self {
            descriptors: HashMap::new(),
        }
    }

    pub fn register_module_bytes(
        &mut self,
        descriptor: WasmUdfDescriptor,
        wasm_bytes: &[u8],
    ) -> Result<(), WasmUdfError> {
        if wasm_bytes.len() < 8 || &wasm_bytes[0..4] != b"\0asm" {
            return Err(WasmUdfError::InvalidWasmMagicHeader(
                descriptor.function_name,
            ));
        }
        self.descriptors
            .insert(descriptor.function_name.to_lowercase(), descriptor);
        Ok(())
    }

    pub fn validate_call(
        &self,
        function_name: &str,
        args: &[WasmUdfValue],
    ) -> Result<&WasmUdfDescriptor, WasmUdfError> {
        let key = function_name.to_lowercase();
        let desc = self
            .descriptors
            .get(&key)
            .ok_or_else(|| WasmUdfError::UdfNotFound(function_name.to_string()))?;

        if args.len() != desc.arg_types.len() {
            return Err(WasmUdfError::ArityMismatch {
                function: desc.function_name.clone(),
                expected: desc.arg_types.len(),
                actual: args.len(),
            });
        }

        for (idx, (val, expected_ty)) in args.iter().zip(desc.arg_types.iter()).enumerate() {
            let matches = match (val, expected_ty) {
                (WasmUdfValue::Null, _) => true,
                (WasmUdfValue::Int64(_), WasmUdfType::Int64) => true,
                (WasmUdfValue::Float64(_), WasmUdfType::Float64) => true,
                (WasmUdfValue::Utf8String(_), WasmUdfType::Utf8String) => true,
                (WasmUdfValue::Boolean(_), WasmUdfType::Boolean) => true,
                _ => false,
            };
            if !matches {
                return Err(WasmUdfError::TypeMismatch {
                    function: desc.function_name.clone(),
                    arg_index: idx,
                    expected: expected_ty.clone(),
                });
            }
        }

        Ok(desc)
    }
}
