use schemars::JsonSchema;

use crate::serde::{Deserialize, Serialize};

/// Deserializes the `udfs` config field, accepting either the list form
/// (`- name: f, config: !Wasm ...`) or the map form from the original proposal
/// (`wasm: { my_function: path/to/module.wasm }`).
pub fn deserialize_udfs<'de, D>(deserializer: D) -> Result<Vec<UdfConfig>, D::Error>
where
    D: crate::serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Udfs {
        List(Vec<UdfConfig>),
        Map {
            wasm: std::collections::BTreeMap<String, String>,
        },
    }

    Ok(match Udfs::deserialize(deserializer)? {
        Udfs::List(udfs) => udfs,
        Udfs::Map { wasm } => wasm
            .into_iter()
            .map(|(name, path)| UdfConfig {
                name,
                config: UdfType::Wasm(WasmConfig { path }),
            })
            .collect(),
    })
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WasmConfig {
    /// path to the WebAssembly module file
    pub path: String,
}
