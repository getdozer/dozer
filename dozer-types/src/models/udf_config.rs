use schemars::JsonSchema;

use crate::serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct UdfConfig {
    /// name of the model function
    pub name: String,
    /// setting for what type of udf to use; Default: Onnx
    pub config: UdfType,
}

impl<'de> Deserialize<'de> for UdfConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Keep the legacy YAML tags and JSON enum objects intact. An untagged
        // enum here would buffer away YAML's !Onnx/!JavaScript/!Wasm tags.
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Input {
            name: String,
            #[serde(default, rename = "type", deserialize_with = "present_kind")]
            kind: Option<WasmKind>,
            config: ConfigBody,
        }
        fn present_kind<'de, D: serde::Deserializer<'de>>(
            d: D,
        ) -> Result<Option<WasmKind>, D::Error> {
            WasmKind::deserialize(d).map(Some)
        }
        let input = Input::deserialize(deserializer)?;
        let config = match (input.kind, input.config) {
            (None, ConfigBody::Tagged(config)) => config,
            (Some(WasmKind::Wasm), ConfigBody::Plain(config)) => UdfType::Wasm(config),
            (None, ConfigBody::Plain(_)) => {
                return Err(serde::de::Error::missing_field("type"));
            }
            (Some(_), ConfigBody::Tagged(_)) => {
                return Err(serde::de::Error::custom(
                    "use either type: wasm with config.path or a tagged config, not both",
                ));
            }
        };
        Ok(Self {
            name: input.name,
            config,
        })
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum WasmKind {
    Wasm,
}

enum ConfigBody {
    Tagged(UdfType),
    Plain(WasmConfig),
}

// YAML's String deserializer may coerce scalars such as null into text.
// The explicit config.path form requires an actual string, as its schema does.
struct PlainPath(String);

impl<'de> Deserialize<'de> for PlainPath {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct PathVisitor;
        impl<'de> serde::de::Visitor<'de> for PathVisitor {
            type Value = PlainPath;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a string containing a WASM module path")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(PlainPath(value.to_owned()))
            }
        }
        deserializer.deserialize_any(PathVisitor)
    }
}

impl<'de> Deserialize<'de> for ConfigBody {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{self, EnumAccess, MapAccess, Visitor};
        struct BodyVisitor;
        impl<'de> Visitor<'de> for BodyVisitor {
            type Value = ConfigBody;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a tagged UDF configuration or a WASM path mapping")
            }
            fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<Self::Value, A::Error> {
                UdfType::deserialize(de::value::EnumAccessDeserializer::new(data))
                    .map(ConfigBody::Tagged)
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let key = map
                    .next_key::<String>()?
                    .ok_or_else(|| de::Error::missing_field("path"))?;
                let config = match key.as_str() {
                    "path" => ConfigBody::Plain(WasmConfig {
                        path: map.next_value::<PlainPath>()?.0,
                    }),
                    "Wasm" => ConfigBody::Tagged(UdfType::Wasm(map.next_value()?)),
                    "Onnx" => ConfigBody::Tagged(UdfType::Onnx(map.next_value()?)),
                    "JavaScript" => ConfigBody::Tagged(UdfType::JavaScript(map.next_value()?)),
                    _ => {
                        return Err(de::Error::unknown_field(
                            &key,
                            &["path", "Wasm", "Onnx", "JavaScript"],
                        ))
                    }
                };
                if let Some(extra) = map.next_key::<String>()? {
                    return Err(de::Error::custom(format!(
                        "unexpected or duplicate config field: {extra}"
                    )));
                }
                Ok(config)
            }
        }
        deserializer.deserialize_any(BodyVisitor)
    }
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
    /// Local module path, relative to the process working directory.
    /// The .wasm extension may be omitted.
    pub path: String,
}

/// Accept tagged/explicit-type lists and the shorthand from issue #1653.
pub fn deserialize_udfs<'de, D>(deserializer: D) -> Result<Vec<UdfConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, MapAccess, SeqAccess, Visitor};
    use std::fmt;

    struct Functions(Vec<UdfConfig>);
    impl<'de> Deserialize<'de> for Functions {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct FunctionsVisitor;
            impl<'de> Visitor<'de> for FunctionsVisitor {
                type Value = Functions;
                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    f.write_str("a mapping of WASM function names to local paths")
                }
                fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                    let mut functions: Vec<UdfConfig> = Vec::new();
                    while let Some((name, path)) = map.next_entry::<String, String>()? {
                        if functions.iter().any(|udf| udf.name == name) {
                            return Err(de::Error::custom(format!(
                                "duplicate WASM function: {name}"
                            )));
                        }
                        functions.push(UdfConfig {
                            name,
                            config: UdfType::Wasm(WasmConfig { path }),
                        });
                    }
                    Ok(Functions(functions))
                }
            }
            d.deserialize_map(FunctionsVisitor)
        }
    }

    struct UdfsVisitor;
    impl<'de> Visitor<'de> for UdfsVisitor {
        type Value = Vec<UdfConfig>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a UDF list or a mapping with a wasm key")
        }
        fn visit_seq<S: SeqAccess<'de>>(self, seq: S) -> Result<Self::Value, S::Error> {
            Vec::<UdfConfig>::deserialize(de::value::SeqAccessDeserializer::new(seq))
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut functions = None;
            while let Some(key) = map.next_key::<String>()? {
                if key != "wasm" {
                    return Err(de::Error::unknown_field(&key, &["wasm"]));
                }
                if functions.is_some() {
                    return Err(de::Error::duplicate_field("wasm"));
                }
                functions = Some(map.next_value::<Functions>()?.0);
            }
            functions.ok_or_else(|| de::Error::missing_field("wasm"))
        }
    }
    deserializer.deserialize_any(UdfsVisitor)
}

pub fn udfs_schema(generator: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    // Only used to describe accepted input; serialization retains the existing list format.
    #[allow(dead_code)]
    #[derive(JsonSchema)]
    #[serde(deny_unknown_fields)]
    struct WasmMapping {
        wasm: std::collections::BTreeMap<String, String>,
    }
    #[allow(dead_code)]
    #[derive(JsonSchema)]
    #[serde(deny_unknown_fields)]
    struct ExplicitWasmUdf {
        name: String,
        #[serde(rename = "type")]
        kind: WasmKind,
        config: WasmConfig,
    }
    #[allow(dead_code)]
    #[derive(JsonSchema)]
    #[serde(untagged)]
    enum UdfEntry {
        Tagged(UdfConfig),
        Explicit(ExplicitWasmUdf),
    }
    #[allow(dead_code)]
    #[derive(JsonSchema)]
    #[serde(untagged)]
    enum UdfInput {
        List(Vec<UdfEntry>),
        Mapping(WasmMapping),
    }
    UdfInput::json_schema(generator)
}
