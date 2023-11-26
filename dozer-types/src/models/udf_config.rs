use schemars::JsonSchema;

use crate::serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct UdfConfig {
    /// name of the model function
    pub name: String,
    /// setting for what type of udf to use; Default: Onnx
    pub config: UdfType,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub enum UdfType {
    Onnx(OnnxConfig),
    JavaScript(JavaScriptConfig),
    Wasm(WasmConfig),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct OnnxConfig {
    /// path to the model file
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JavaScriptConfig {
    /// path to the module file
    pub module: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
pub struct WasmConfig {
    #[prost(string)]
    /// path to the module file
    pub path: String,
}
