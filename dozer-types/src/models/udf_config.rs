use std::collections::BTreeMap;

use schemars::JsonSchema;

use crate::serde::{
    de::{self, Deserializer, SeqAccess, Visitor},
    Deserialize, Serialize,
};

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
    /// path to the wasm module file (with or without .wasm extension)
    #[serde(alias = "module")]
    pub path: String,
    /// optional exported function name inside the wasm module (defaults to UdfConfig.name)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UdfsMapFormat {
    #[serde(default)]
    wasm: BTreeMap<String, String>,
}

pub fn deserialize_udfs<'de, D>(deserializer: D) -> Result<Vec<UdfConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    struct UdfsVisitor;

    impl<'de> Visitor<'de> for UdfsVisitor {
        type Value = Vec<UdfConfig>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a sequence of UdfConfig or a map containing wasm UDF definitions")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut vec = Vec::new();
            while let Some(elem) = seq.next_element()? {
                vec.push(elem);
            }
            Ok(vec)
        }

        fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
        where
            M: de::MapAccess<'de>,
        {
            let map_format =
                UdfsMapFormat::deserialize(de::value::MapAccessDeserializer::new(map))?;
            Ok(map_format
                .wasm
                .into_iter()
                .map(|(name, path)| UdfConfig {
                    name,
                    config: UdfType::Wasm(WasmConfig {
                        path,
                        function: None,
                    }),
                })
                .collect())
        }
    }

    deserializer.deserialize_any(UdfsVisitor)
}
