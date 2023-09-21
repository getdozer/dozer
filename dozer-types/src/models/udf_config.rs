use schemars::JsonSchema;

use crate::serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, Eq, PartialEq, Clone)]
pub struct UdfConfig {
    /// name of the model function
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    /// setting for what type of udf to use; Default: Onnx
    pub config: Option<UdfType>,
}

#[derive(Debug, Serialize, JsonSchema, Deserialize, Eq, PartialEq, Clone)]
pub enum UdfType {
    Onnx(OnnxConfig),
}

#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, Eq, PartialEq, Clone)]
pub struct OnnxConfig {
    /// path to the model file
    pub path: String,
}
