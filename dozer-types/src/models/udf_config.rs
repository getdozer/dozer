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

/// Numeric WebAssembly scalar function. Relative paths use the Dozer project directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WasmConfig {
    pub path: String,
    /// Export name; defaults to the configured SQL function name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub export: Option<String>,
    /// Instruction budget including module initialization, for each SQL evaluation.
    #[serde(default = "default_wasm_fuel")]
    #[schemars(range(min = 1, max = 10000000))]
    pub fuel: u64,
    /// Maximum linear memory size, in bytes.
    #[serde(default = "default_wasm_memory")]
    #[schemars(range(min = 1, max = 67108864))]
    pub memory_limit: usize,
}

pub fn default_wasm_fuel() -> u64 {
    1_000_000
}

pub fn default_wasm_memory() -> usize {
    16 * 1024 * 1024
}

#[derive(JsonSchema)]
#[serde(untagged)]
pub enum UdfConfigurations {
    List(Vec<UdfConfig>),
    WasmMap(WasmUdfMap),
}

#[derive(JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WasmUdfMap {
    pub wasm: std::collections::BTreeMap<String, String>,
}

impl WasmConfig {
    pub fn new(path: String) -> Self {
        Self {
            path,
            export: None,
            fuel: default_wasm_fuel(),
            memory_limit: default_wasm_memory(),
        }
    }
}

/// Accept the original `udfs: { wasm: { function: path } }` proposal as well as
/// the existing list of tagged UDF configurations. A visitor preserves YAML tags
/// in list entries, including existing !Onnx and !JavaScript configurations.
pub fn deserialize_udfs<'de, D>(deserializer: D) -> Result<Vec<UdfConfig>, D::Error>
where
    D: crate::serde::Deserializer<'de>,
{
    use crate::serde::de::{value::SeqAccessDeserializer, Error, MapAccess, SeqAccess, Visitor};
    use std::{collections::BTreeMap, fmt};

    struct WasmPaths(BTreeMap<String, String>);
    impl<'de> Deserialize<'de> for WasmPaths {
        fn deserialize<D: crate::serde::Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Self, D::Error> {
            struct PathsVisitor;
            impl<'de> Visitor<'de> for PathsVisitor {
                type Value = WasmPaths;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("a wasm function-to-path mapping")
                }

                fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                    let mut paths = BTreeMap::new();
                    while let Some((name, path)) = map.next_entry::<String, String>()? {
                        if paths.insert(name.clone(), path).is_some() {
                            return Err(A::Error::custom(format!(
                                "duplicate wasm function `{name}`"
                            )));
                        }
                    }
                    Ok(WasmPaths(paths))
                }
            }
            deserializer.deserialize_map(PathsVisitor)
        }
    }

    struct UdfsVisitor;
    impl<'de> Visitor<'de> for UdfsVisitor {
        type Value = Vec<UdfConfig>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a UDF list or a wasm function-to-path mapping")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
            Vec::deserialize(SeqAccessDeserializer::new(seq))
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut result = Vec::new();
            let mut seen = false;
            while let Some(key) = map.next_key::<String>()? {
                if key != "wasm" {
                    return Err(A::Error::unknown_field(&key, &["wasm"]));
                }
                if seen {
                    return Err(A::Error::duplicate_field("wasm"));
                }
                seen = true;
                for (name, path) in map.next_value::<WasmPaths>()?.0 {
                    result.push(UdfConfig {
                        name,
                        config: UdfType::Wasm(WasmConfig::new(path)),
                    });
                }
            }
            if !seen {
                return Err(A::Error::missing_field("wasm"));
            }
            Ok(result)
        }
    }
    deserializer.deserialize_any(UdfsVisitor)
}
